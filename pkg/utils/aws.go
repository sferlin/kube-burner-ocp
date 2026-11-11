// Copyright 2025 The Kube-burner Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// BCCRouteServerObserver provides methods to interact with AWS VPC Route Server.
// It implements the BCCRouteServerProvider interface.
type BCCRouteServerObserver struct {
	client        *ec2.Client
	routeServerID string
	peerCount     int // available route server peers, counted once at startup

	mu            sync.Mutex
	ribSeen       sightings                  // prefix -> peer -> first poll with a routing database entry from that peer
	installSeen   sightings                  // prefix -> route table -> first poll reporting the route installed there
	installTables map[string]map[string]bool // prefix -> route tables the route server installs it into
}

// NewBCCRouteServerObserver creates a new AWS Route Server observer that implements BCCRouteServerProvider.
func NewBCCRouteServerObserver(region, routeServerID string) (*BCCRouteServerObserver, error) {
	if region == "" {
		return nil, fmt.Errorf("AWS region is empty")
	}
	if routeServerID == "" {
		return nil, fmt.Errorf("AWS Route Server ID is empty")
	}

	// Loads credentials, region, profile, IAM role, etc.
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS config: %w", err)
	}

	o := &BCCRouteServerObserver{
		client:        ec2.NewFromConfig(cfg),
		routeServerID: routeServerID,
		ribSeen:       sightings{},
		installSeen:   sightings{},
		installTables: map[string]map[string]bool{},
	}
	if o.peerCount, err = o.countPeers(context.Background()); err != nil {
		return nil, err
	}
	return o, nil
}

// countPeers returns the number of available peers of the route server.
func (o *BCCRouteServerObserver) countPeers(ctx context.Context) (int, error) {
	count := 0
	var nextToken *string
	for {
		output, err := o.client.DescribeRouteServerPeers(ctx, &ec2.DescribeRouteServerPeersInput{NextToken: nextToken})
		if err != nil {
			return 0, fmt.Errorf("describe route server peers: %w", err)
		}
		for _, peer := range output.RouteServerPeers {
			if aws.ToString(peer.RouteServerId) == o.routeServerID && peer.State == ec2types.RouteServerPeerStateAvailable {
				count++
			}
		}
		if aws.ToString(output.NextToken) == "" {
			break
		}
		nextToken = output.NextToken
	}
	if count == 0 {
		return 0, fmt.Errorf("route server %s has no available peers", o.routeServerID)
	}
	return count, nil
}

// GetRoutes retrieves the AWS Route Server routing database, which has one entry per prefix and peer, and returns
// one RouteInfo per prefix. FirstSeen/AllSeen record the first poll with an entry from any/every peer (T1/T1.5).
// The entries' route table installation status is kept for CheckRouteTables (T3.5).
func (o *BCCRouteServerObserver) GetRoutes(ctx context.Context) ([]RouteInfo, error) {
	var awsRoutes []ec2types.RouteServerRoute
	var nextToken *string

	for {
		input := &ec2.GetRouteServerRoutingDatabaseInput{
			RouteServerId: aws.String(o.routeServerID),
			MaxResults:    aws.Int32(1000),
			NextToken:     nextToken,
		}

		output, err := o.client.GetRouteServerRoutingDatabase(ctx, input)
		if err != nil {
			return nil, fmt.Errorf(
				"get route server routing database: %w",
				err,
			)
		}

		awsRoutes = append(awsRoutes, output.Routes...)

		if output.NextToken == nil || aws.ToString(output.NextToken) == "" {
			break
		}

		nextToken = output.NextToken
	}

	now := time.Now().UTC()
	o.mu.Lock()
	defer o.mu.Unlock()

	inFIB := map[string]bool{}
	for _, route := range awsRoutes {
		prefix := aws.ToString(route.Prefix)
		inFIB[prefix] = inFIB[prefix] || route.RouteStatus == ec2types.RouteServerRouteStatusInFib
		o.ribSeen.record(prefix, aws.ToString(route.RouteServerPeerId), now)
		for _, detail := range route.RouteInstallationDetails {
			table := aws.ToString(detail.RouteTableId)
			if o.installTables[prefix] == nil {
				o.installTables[prefix] = map[string]bool{}
			}
			o.installTables[prefix][table] = true
			if detail.RouteInstallationStatus == ec2types.RouteServerRouteInstallationStatusInstalled {
				o.installSeen.record(prefix, table, now)
			}
		}
	}

	routes := make([]RouteInfo, 0, len(inFIB))
	for prefix, fib := range inFIB {
		first, all := o.ribSeen.span(prefix, o.peerCount)
		routes = append(routes, RouteInfo{Prefix: prefix, InFIB: fib, FirstSeen: first, AllSeen: all})
	}
	return routes, nil
}

// CheckRouteTables checks which subnets are present in VPC route tables.
func (o *BCCRouteServerObserver) CheckRouteTables(ctx context.Context, vpcID string, subnets []string) (map[string]RouteTableStatus, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("VPC ID not configured")
	}

	input := &ec2.DescribeRouteTablesInput{
		Filters: []ec2types.Filter{
			{
				Name:   aws.String("vpc-id"),
				Values: []string{vpcID},
			},
		},
	}

	output, err := o.client.DescribeRouteTables(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("describe route tables: %w", err)
	}
	now := time.Now().UTC()

	// Map of subnet → exists in VPC route table
	found := make(map[string]RouteTableStatus)
	for _, subnet := range subnets {
		found[subnet] = RouteTableStatus{}
	}

	// Check each route table for our subnets
	for _, rt := range output.RouteTables {
		for _, route := range rt.Routes {
			destCidr := aws.ToString(route.DestinationCidrBlock)
			if _, checking := found[destCidr]; checking {
				found[destCidr] = RouteTableStatus{FirstSeen: now}
			}
		}
	}

	// T3.5: every route table the route server installs the route into reports it installed,
	// as recorded from the routing database by GetRoutes
	o.mu.Lock()
	defer o.mu.Unlock()
	for subnet, status := range found {
		_, status.AllSeen = o.installSeen.span(subnet, len(o.installTables[subnet]))
		found[subnet] = status
	}

	return found, nil
}

// ValidateAWSConnectivity validates AWS credentials and connectivity to Route Server and VPC route tables.
func ValidateAWSConnectivity(region, routeServerID, vpcID string) error {
	observer, err := NewBCCRouteServerObserver(region, routeServerID)
	if err != nil {
		return fmt.Errorf("invalid AWS credentials or region (check credentials, region, and route-server-id): %w", err)
	}

	return ValidateConnectivity("AWS", observer, vpcID)
}
