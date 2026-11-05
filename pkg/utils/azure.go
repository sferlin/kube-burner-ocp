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
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	armnetwork "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v5"
	log "github.com/sirupsen/logrus"
)

// AzureRouteServerProvider provides methods to interact with Azure Route Server.
// It implements the BCCRouteServerProvider interface.
// Azure BGP route flow:
//   - T1: Route Server learned BGP routes (via BeginListLearnedRoutes)
//   - T2+T3: Network Interface effective routes (via BeginGetEffectiveRouteTable)
type AzureRouteServerProvider struct {
	bgpClient       *armnetwork.VirtualHubBgpConnectionsClient
	nicClient       *armnetwork.InterfacesClient
	routeServerName string
	routeServerRG   string
	nicResourceID   string
	subscriptionID  string
}

// NewAzureRouteServerProvider creates a new Azure Route Server provider.
// nicResourceID is the Route Server's NIC resource ID (format: /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Network/networkInterfaces/{nic}).
func NewAzureRouteServerProvider(subscriptionID, routeServerRG, routeServerName, nicResourceID string) (*AzureRouteServerProvider, error) {
	if err := validateAzureParams(subscriptionID, routeServerRG, routeServerName); err != nil {
		return nil, err
	}

	// Create credential (uses DefaultAzureCredential which tries multiple auth methods)
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create Azure credentials: %w", err)
	}

	// Create Virtual Hub BGP Connections client for querying learned routes (T1)
	bgpClient, err := armnetwork.NewVirtualHubBgpConnectionsClient(subscriptionID, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create Azure Virtual Hub BGP Connections client: %w", err)
	}

	// Create Network Interfaces client for querying effective routes (T2+T3)
	nicClient, err := armnetwork.NewInterfacesClient(subscriptionID, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create Azure Network Interfaces client: %w", err)
	}

	return &AzureRouteServerProvider{
		bgpClient:       bgpClient,
		nicClient:       nicClient,
		routeServerName: routeServerName,
		routeServerRG:   routeServerRG,
		nicResourceID:   nicResourceID,
		subscriptionID:  subscriptionID,
	}, nil
}

// GetRoutes retrieves all routes learned by the Azure Route Server (T1/RIB).
// Queries the Route Server's BGP RIB directly via VirtualHubBgpConnectionsClient.BeginListLearnedRoutes.
// Returns routes learned from all BGP connections as cloud-agnostic RouteInfo objects.
func (p *AzureRouteServerProvider) GetRoutes(ctx context.Context) ([]RouteInfo, error) {
	var allRoutes []RouteInfo

	// List all BGP connections on the Route Server and query learned routes from each
	connPager := p.bgpClient.NewListPager(p.routeServerRG, p.routeServerName, nil)

	for connPager.More() {
		connPage, err := connPager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list BGP connections: %w", err)
		}

		for _, conn := range connPage.Value {
			if conn == nil || conn.Name == nil {
				continue
			}

			poller, err := p.bgpClient.BeginListLearnedRoutes(ctx, p.routeServerRG, p.routeServerName, *conn.Name, nil)
			if err != nil {
				log.Warnf("failed to query learned routes from peer %s: %v", *conn.Name, err)
				continue
			}

			res, err := poller.PollUntilDone(ctx, nil)
			if err != nil {
				log.Warnf("failed to poll learned routes from peer %s: %v", *conn.Name, err)
				continue
			}

			for _, routeList := range res.Value {
				for _, route := range routeList {
					if route != nil && route.Network != nil {
						allRoutes = append(allRoutes, RouteInfo{
							Prefix: *route.Network,
							InFIB:  false, // Learned routes are in RIB, not FIB
						})
					}
				}
			}
		}
	}

	return allRoutes, nil
}

// CheckRouteTables checks which subnets are present in the Route Server's NIC effective routes (T2+T3).
// resourceID is the Route Server's NIC resource ID (format: /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Network/networkInterfaces/{nic}).
// If empty, uses the provider's configured nicResourceID.
func (p *AzureRouteServerProvider) CheckRouteTables(ctx context.Context, resourceID string, subnets []string) (map[string]bool, error) {
	if len(subnets) == 0 {
		return make(map[string]bool), nil
	}

	// Use provided resourceID or fall back to configured NIC resource ID
	nicID := resourceID
	if nicID == "" {
		nicID = p.nicResourceID
	}

	if nicID == "" {
		return nil, fmt.Errorf("route server NIC resource ID is required")
	}

	// Parse NIC resource ID to extract resource group and NIC name
	// Format: /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Network/networkInterfaces/{nic}
	parts := parseResourceID(nicID)
	if parts["rg"] == "" || parts["nic"] == "" {
		return nil, fmt.Errorf("invalid NIC resource ID format: %s", nicID)
	}

	// Initialize map with all subnets marked as not found
	found := make(map[string]bool)
	for _, subnet := range subnets {
		found[subnet] = false
	}

	// Query effective routes on the Route Server's NIC (T2+T3: BGP routes installed for forwarding)
	poller, err := p.nicClient.BeginGetEffectiveRouteTable(ctx, parts["rg"], parts["nic"], nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query effective routes on Route Server NIC: %w", err)
	}

	res, err := poller.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to poll effective routes: %w", err)
	}

	// Check if any of our target subnets appear in effective routes
	for _, route := range res.Value {
		if route == nil || route.AddressPrefix == nil {
			continue
		}
		for _, prefix := range route.AddressPrefix {
			if prefix != nil {
				if _, checking := found[*prefix]; checking {
					found[*prefix] = true
				}
			}
		}
	}

	return found, nil
}

// parseResourceID extracts components from an Azure resource ID.
// Format: /subscriptions/{sub}/resourceGroups/{rg}/providers/{provider}/{type}/{name}
func parseResourceID(id string) map[string]string {
	parts := map[string]string{}
	segments := strings.Split(strings.TrimPrefix(id, "/"), "/")

	for i := 0; i < len(segments)-1; i += 2 {
		switch segments[i] {
		case "resourceGroups":
			parts["rg"] = segments[i+1]
		case "networkInterfaces":
			parts["nic"] = segments[i+1]
		}
	}

	return parts
}

// ValidateAzureConnectivity validates Azure credentials and connectivity to Route Server and route tables.
func ValidateAzureConnectivity(subscriptionID, resourceGroup, routeServerName string) error {
	provider, err := NewAzureRouteServerProvider(subscriptionID, resourceGroup, routeServerName, "")
	if err != nil {
		return fmt.Errorf("invalid Azure credentials (check credentials, subscription, resource group, and route server name): %w", err)
	}

	return ValidateConnectivity("Azure", provider, "")
}

// validateAzureParams validates required Azure parameters.
func validateAzureParams(subscriptionID, resourceGroup, routeServerName string) error {
	if subscriptionID == "" {
		return fmt.Errorf("azure subscription ID is empty")
	}
	if resourceGroup == "" {
		return fmt.Errorf("azure resource group is empty")
	}
	if routeServerName == "" {
		return fmt.Errorf("azure route server name is empty")
	}
	return nil
}

// Ensure AzureRouteServerProvider implements BCCRouteServerProvider
var _ BCCRouteServerProvider = (*AzureRouteServerProvider)(nil)
