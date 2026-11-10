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
	"slices"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	armnetwork "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v5"
	log "github.com/sirupsen/logrus"
)

// AzureRouteServerProvider implements BCCRouteServerProvider for Azure Route Server.
// T1 comes from the Route Server's BGP learned routes. Route table presence comes from the
// effective routes of the BGP peer NICs, since Azure exposes no separate FIB state.
type AzureRouteServerProvider struct {
	bgpClient       *armnetwork.VirtualHubBgpConnectionsClient
	nicClient       *armnetwork.InterfacesClient
	routeServerName string
	routeServerRG   string
	peerNICs        []*arm.ResourceID
}

// NewAzureRouteServerProvider creates a new Azure Route Server provider and discovers the NICs of its BGP peers.
func NewAzureRouteServerProvider(subscriptionID, routeServerRG, routeServerName string) (*AzureRouteServerProvider, error) {
	if subscriptionID == "" || routeServerRG == "" || routeServerName == "" {
		return nil, fmt.Errorf("azure subscription ID, resource group and route server name are required")
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create Azure credentials: %w", err)
	}

	bgpClient, err := armnetwork.NewVirtualHubBgpConnectionsClient(subscriptionID, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create Azure BGP connections client: %w", err)
	}

	nicClient, err := armnetwork.NewInterfacesClient(subscriptionID, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create Azure network interfaces client: %w", err)
	}

	ipConfigClient, err := armnetwork.NewVirtualHubIPConfigurationClient(subscriptionID, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create Azure route server IP configuration client: %w", err)
	}

	p := &AzureRouteServerProvider{
		bgpClient:       bgpClient,
		nicClient:       nicClient,
		routeServerName: routeServerName,
		routeServerRG:   routeServerRG,
	}
	if p.peerNICs, err = p.discoverPeerNICs(context.Background(), ipConfigClient); err != nil {
		return nil, err
	}
	nicsByRG := map[string][]string{}
	for _, nic := range p.peerNICs {
		nicsByRG[nic.ResourceGroupName] = append(nicsByRG[nic.ResourceGroupName], nic.Name)
	}
	for rg, names := range nicsByRG {
		slices.Sort(names)
		log.Infof("Using %d BGP peer NICs in resource group %s for route table detection: %s", len(names), rg, strings.Join(names, ", "))
	}

	return p, nil
}

// discoverPeerNICs maps each BGP peer IP of the Route Server to the NIC holding that private IP
// in the Route Server's VNet. Private IPs are only unique within a VNet.
func (p *AzureRouteServerProvider) discoverPeerNICs(ctx context.Context, ipConfigClient *armnetwork.VirtualHubIPConfigurationClient) ([]*arm.ResourceID, error) {
	vnet, err := p.routeServerVNet(ctx, ipConfigClient)
	if err != nil {
		return nil, err
	}
	peerIPs, err := p.peerIPs(ctx)
	if err != nil {
		return nil, err
	}
	nicsByIP, err := p.nicsByIP(ctx, vnet, peerIPs)
	if err != nil {
		return nil, err
	}

	var nics []*arm.ResourceID
	for ip := range peerIPs {
		ids := nicsByIP[ip]
		if len(ids) != 1 {
			return nil, fmt.Errorf("BGP peer IP %s must match exactly one NIC in VNet %s, found %d: %v", ip, vnet, len(ids), ids)
		}
		nic, err := arm.ParseResourceID(ids[0])
		if err != nil {
			return nil, fmt.Errorf("invalid NIC resource ID %q: %w", ids[0], err)
		}
		nics = append(nics, nic)
	}
	return nics, nil
}

// routeServerVNet returns the VNet the Route Server is deployed in, taken from its IP configuration subnet.
func (p *AzureRouteServerProvider) routeServerVNet(ctx context.Context, ipConfigClient *armnetwork.VirtualHubIPConfigurationClient) (string, error) {
	pager := ipConfigClient.NewListPager(p.routeServerRG, p.routeServerName, nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to read route server IP configuration: %w", err)
		}
		for _, ipConfig := range page.Value {
			if ipConfig != nil && ipConfig.Properties != nil && ipConfig.Properties.Subnet != nil && ipConfig.Properties.Subnet.ID != nil {
				return vnetOf(*ipConfig.Properties.Subnet.ID), nil
			}
		}
	}
	return "", fmt.Errorf("could not determine the VNet of route server %s", p.routeServerName)
}

// peerIPs returns the IPs of the Route Server's BGP peers.
func (p *AzureRouteServerProvider) peerIPs(ctx context.Context) (map[string]bool, error) {
	ips := map[string]bool{}
	pager := p.bgpClient.NewListPager(p.routeServerRG, p.routeServerName, nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list BGP connections: %w", err)
		}
		for _, conn := range page.Value {
			if conn != nil && conn.Properties != nil && conn.Properties.PeerIP != nil {
				ips[*conn.Properties.PeerIP] = true
			}
		}
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("route server %s has no BGP peers", p.routeServerName)
	}
	return ips, nil
}

// nicsByIP returns the IDs of the NICs in vnet that hold one of the given private IPs, keyed by IP.
func (p *AzureRouteServerProvider) nicsByIP(ctx context.Context, vnet string, ips map[string]bool) (map[string][]string, error) {
	nics := map[string][]string{}
	pager := p.nicClient.NewListAllPager(nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list network interfaces: %w", err)
		}
		for _, nic := range page.Value {
			if nic == nil || nic.ID == nil || nic.Properties == nil {
				continue
			}
			for _, ipConfig := range nic.Properties.IPConfigurations {
				if ipConfig == nil || ipConfig.Properties == nil || ipConfig.Properties.PrivateIPAddress == nil ||
					ipConfig.Properties.Subnet == nil || ipConfig.Properties.Subnet.ID == nil {
					continue
				}
				ip := *ipConfig.Properties.PrivateIPAddress
				if ips[ip] && vnetOf(*ipConfig.Properties.Subnet.ID) == vnet {
					nics[ip] = append(nics[ip], *nic.ID)
				}
			}
		}
	}
	return nics, nil
}

// vnetOf returns the lower-cased VNet resource ID of a subnet resource ID.
func vnetOf(subnetID string) string {
	id := strings.ToLower(subnetID)
	if i := strings.Index(id, "/subnets/"); i >= 0 {
		return id[:i]
	}
	return id
}

// GetRoutes returns the routes learned by the Route Server from all of its BGP peers (T1).
func (p *AzureRouteServerProvider) GetRoutes(ctx context.Context) ([]RouteInfo, error) {
	var routes []RouteInfo

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

			// Value maps each virtual router instance to the routes it learned
			for _, peerRoutes := range res.Value {
				for _, route := range peerRoutes {
					if route != nil && route.Network != nil {
						routes = append(routes, RouteInfo{Prefix: *route.Network})
					}
				}
			}
		}
	}

	return routes, nil
}

// CheckRouteTables reports, for each subnet, whether it is in the effective routes of at least
// one BGP peer NIC (Any) and of every BGP peer NIC (All). All peer NICs are queried in parallel.
func (p *AzureRouteServerProvider) CheckRouteTables(ctx context.Context, _ string, subnets []string) (map[string]RouteTableStatus, error) {
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		firstErr error
		counts   = map[string]int{}
	)
	for _, nic := range p.peerNICs {
		wg.Go(func() {
			prefixes, err := p.effectiveRoutes(ctx, nic)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			for _, subnet := range subnets {
				if prefixes[subnet] {
					counts[subnet]++
				}
			}
		})
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	found := make(map[string]RouteTableStatus, len(subnets))
	for _, subnet := range subnets {
		found[subnet] = RouteTableStatus{Any: counts[subnet] > 0, All: counts[subnet] == len(p.peerNICs)}
	}
	return found, nil
}

// effectiveRoutes returns the address prefixes in a NIC's effective route table.
func (p *AzureRouteServerProvider) effectiveRoutes(ctx context.Context, nic *arm.ResourceID) (map[string]bool, error) {
	poller, err := p.nicClient.BeginGetEffectiveRouteTable(ctx, nic.ResourceGroupName, nic.Name, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query effective routes of NIC %s: %w", nic.Name, err)
	}
	res, err := poller.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to poll effective routes of NIC %s: %w", nic.Name, err)
	}

	prefixes := map[string]bool{}
	for _, route := range res.Value {
		if route == nil {
			continue
		}
		for _, prefix := range route.AddressPrefix {
			if prefix != nil {
				prefixes[*prefix] = true
			}
		}
	}
	return prefixes, nil
}

// ValidateAzureConnectivity validates Azure credentials, BGP peer NIC discovery, and access to the peers' effective routes.
func ValidateAzureConnectivity(subscriptionID, resourceGroup, routeServerName string) error {
	provider, err := NewAzureRouteServerProvider(subscriptionID, resourceGroup, routeServerName)
	if err != nil {
		return fmt.Errorf("invalid Azure configuration (check credentials, subscription, resource group, and route server name): %w", err)
	}

	return ValidateConnectivity("Azure", provider, "")
}
