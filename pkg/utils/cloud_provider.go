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
	"time"

	log "github.com/sirupsen/logrus"
)

// RouteInfo represents a cloud-agnostic route in a Route Server's routing database.
type RouteInfo struct {
	Prefix string // CIDR block (e.g., "10.0.0.0/16")
	InFIB  bool   // Whether the route is active in the Forwarding Information Base

	// Only set by providers that query the RIB per BGP peer (Azure); zero otherwise.
	FirstSeen time.Time // first peer that reported the route
	AllSeen   time.Time // when the last peer first reported it, once every peer has
}

// BCCRouteServerProvider defines the interface for cloud-specific Route Server operations.
// Implementations exist for AWS Route Server and Azure Route Server.
type BCCRouteServerProvider interface {
	// GetRoutes retrieves all routes from the Route Server's routing database.
	GetRoutes(ctx context.Context) ([]RouteInfo, error)

	// CheckRouteTables checks which subnets are present in cloud-specific routing resources.
	// For AWS: checks the route tables of the VPC given by resourceID.
	// For Azure: checks the effective routes of every BGP peer NIC; resourceID is unused.
	CheckRouteTables(ctx context.Context, resourceID string, subnets []string) (map[string]RouteTableStatus, error)
}

// RouteTableStatus reports when a subnet was first found; zero times mean not found yet. AllSeen is only
// set by providers that check several locations which must each receive the route (Azure BGP peer NICs).
type RouteTableStatus struct {
	FirstSeen time.Time // first found in any route table
	AllSeen   time.Time // found in every route table checked (when the last one first had it)
}

// ValidateConnectivity tests a Route Server provider's connectivity.
// cloud is the provider name (aws, azure) for logging purposes.
func ValidateConnectivity(cloud string, provider BCCRouteServerProvider, resourceID string) error {
	log.Infof("Validating %s Route Server connectivity", cloud)

	ctx := context.Background()

	// Test Route Server connectivity
	log.Infof("Testing Route Server API...")
	routes, err := provider.GetRoutes(ctx)
	if err != nil {
		return fmt.Errorf("failed to query Route Server: %w", err)
	}
	log.Infof("Successfully connected. Found %d routes.", len(routes))

	// Test route table connectivity
	log.Infof("Testing route table API...")
	_, err = provider.CheckRouteTables(ctx, resourceID, []string{})
	if err != nil {
		return fmt.Errorf("failed to query route tables: %w", err)
	}
	log.Infof("Successfully connected to route tables.")

	return nil
}
