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

	return &BCCRouteServerObserver{
		client:        ec2.NewFromConfig(cfg),
		routeServerID: routeServerID,
	}, nil
}

// GetRoutes retrieves all routes from the AWS Route Server routing database and returns them as cloud-agnostic RouteInfo objects.
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

	// Convert AWS-specific types to cloud-agnostic RouteInfo
	routes := make([]RouteInfo, len(awsRoutes))
	for i, route := range awsRoutes {
		routes[i] = RouteInfo{
			Prefix: aws.ToString(route.Prefix),
			InFIB:  route.RouteStatus == ec2types.RouteServerRouteStatusInFib,
		}
	}

	return routes, nil
}

// CheckRouteTables checks which subnets are present in VPC route tables.
// resourceID must be the VPC ID.
func (o *BCCRouteServerObserver) CheckRouteTables(ctx context.Context, resourceID string, subnets []string) (map[string]bool, error) {
	vpcId := resourceID
	if vpcId == "" {
		return nil, fmt.Errorf("VPC ID not configured")
	}

	input := &ec2.DescribeRouteTablesInput{
		Filters: []ec2types.Filter{
			{
				Name:   aws.String("vpc-id"),
				Values: []string{vpcId},
			},
		},
	}

	output, err := o.client.DescribeRouteTables(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("describe route tables: %w", err)
	}

	// Map of subnet → exists in VPC route table
	found := make(map[string]bool)
	for _, subnet := range subnets {
		found[subnet] = false
	}

	// Check each route table for our subnets
	for _, rt := range output.RouteTables {
		for _, route := range rt.Routes {
			destCidr := aws.ToString(route.DestinationCidrBlock)
			if _, checking := found[destCidr]; checking {
				found[destCidr] = true
			}
		}
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
