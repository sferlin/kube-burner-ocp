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

package workloads

import (
	"context"
	"fmt"
	"os"
	"time"

	kubeburnermeasurements "github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/workloads"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kube-burner/kube-burner-ocp/pkg/measurements"
	"github.com/kube-burner/kube-burner-ocp/pkg/utils"
)

var additionalMeasurementFactoryMapBCC = map[string]kubeburnermeasurements.NewMeasurementFactory{
	"raLatencyBCC": measurements.NewRaLatencyBCCMeasurementFactory,
}

var (
	bgpRoutingGVR = schema.GroupVersionResource{
		Group:    "networking.openshift.io",
		Version:  "v1beta1",
		Resource: "bgproutings",
	}
)

// cleanupBCCResources deletes BGP Cloud Connector resources in the correct order:
// 1. BGPRouting CRs (automatically cleans up their managed CUDNs via finalizers and withdraws routes)
// 2. Namespaces (cascade deletes pods and other namespaced resources)
func cleanupBCCResources(ctx context.Context, labelSelector string) {
	k8sConnector := getK8SConnector()

	log.Infof("Cleaning up BGPRouting CRs with label: %s", labelSelector)
	err := k8sConnector.DynamicClient().Resource(bgpRoutingGVR).DeleteCollection(
		ctx,
		metav1.DeleteOptions{},
		metav1.ListOptions{LabelSelector: labelSelector},
	)
	if err != nil {
		log.Warnf("Error deleting BGPRouting CRs: %v", err)
	}

	// Wait for BGPRouting finalizers to clean up CUDNs and for BGP routes to be withdrawn
	time.Sleep(10 * time.Second)

	log.Infof("Cleaning up namespaces with label: %s", labelSelector)
	cleanupTestNamespaces(ctx, labelSelector)
}

// NewUdnBgpBCC creates the BGP Cloud Connector variant of the udn-bgp workload.
// It supports AWS VPC Route Server and Azure Route Server.
func NewUdnBgpBCC(wh *workloads.WorkloadHelper, variant string) *cobra.Command {
	var iterations int
	var metricsProfiles []string
	var cloudProvider string

	// AWS-specific variables
	var awsRegion string
	var routeServerID string
	var vpcId string

	// Azure-specific variables
	var azureSubscriptionID string
	var azureResourceGroup string
	var azureRouteServerName string
	var azureNicResourceID string

	var rc int
	cmd := &cobra.Command{
		Use:   variant,
		Short: fmt.Sprintf("Runs %v workload", variant),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Clean up resources from previous runs in the correct order
			ctx := context.Background()
			cleanupBCCResources(ctx, "kube-burner.io/job")

			switch cloudProvider {
			case "aws":
				if routeServerID == "" || awsRegion == "" || vpcId == "" {
					return fmt.Errorf("--route-server-id, --aws-region, and --vpc-id are required")
				}
				if err := utils.ValidateAWSConnectivity(awsRegion, routeServerID, vpcId); err != nil {
					return err
				}
			case "azure":
				if azureSubscriptionID == "" || azureResourceGroup == "" || azureRouteServerName == "" {
					return fmt.Errorf("--azure-subscription-id, --azure-resource-group, and --azure-route-server-name are required")
				}
				if err := utils.ValidateAzureConnectivity(azureSubscriptionID, azureResourceGroup, azureRouteServerName); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unsupported cloud provider: %s (must be aws or azure)", cloudProvider)
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			setMetrics(cmd, metricsProfiles)
			AdditionalVars["JOB_ITERATIONS"] = iterations
			AdditionalVars["CLOUD_PROVIDER"] = cloudProvider

			switch cloudProvider {
			case "aws":
				AdditionalVars["AWS_REGION"] = awsRegion
				AdditionalVars["ROUTE_SERVER_ID"] = routeServerID
				AdditionalVars["VPC_ID"] = vpcId
			case "azure":
				AdditionalVars["AZURE_SUBSCRIPTION_ID"] = azureSubscriptionID
				AdditionalVars["AZURE_RESOURCE_GROUP"] = azureResourceGroup
				AdditionalVars["AZURE_ROUTE_SERVER_NAME"] = azureRouteServerName
				AdditionalVars["AZURE_NIC_RESOURCE_ID"] = azureNicResourceID
			}

			wh.SetMeasurements(additionalMeasurementFactoryMapBCC)
			rc = RunWorkload(cmd, wh, cmd.Name()+".yml")
		},
		PostRun: func(cmd *cobra.Command, args []string) {
			// Clean up resources if GC is enabled
			if SetVars["GC"] == "true" {
				ctx := context.Background()
				cleanupBCCResources(ctx, "kube-burner.io/job")
			}
			os.Exit(rc)
		},
	}
	cmd.Flags().IntVar(&iterations, "iterations", 10, fmt.Sprintf("%v iterations", variant))
	cmd.Flags().StringSliceVar(&metricsProfiles, "metrics-profile", []string{"metrics.yml"}, "Comma separated list of metrics profiles to use")
	cmd.Flags().StringVar(&cloudProvider, "cloud-provider", "aws", "Cloud provider: aws or azure")
	cmd.MarkFlagRequired("iterations")

	// AWS-specific flags
	cmd.Flags().StringVar(&awsRegion, "aws-region", "", "[AWS] AWS region containing the Route Server")
	cmd.Flags().StringVar(&routeServerID, "route-server-id", "", "[AWS] AWS Route Server ID")
	cmd.Flags().StringVar(&vpcId, "vpc-id", "", "[AWS] AWS VPC ID for route table verification (T3 measurement)")

	// Azure-specific flags
	cmd.Flags().StringVar(&azureSubscriptionID, "azure-subscription-id", "", "[Azure] Azure subscription ID")
	cmd.Flags().StringVar(&azureResourceGroup, "azure-resource-group", "", "[Azure] Azure resource group containing Route Server")
	cmd.Flags().StringVar(&azureRouteServerName, "azure-route-server-name", "", "[Azure] Azure Route Server name")
	cmd.Flags().StringVar(&azureNicResourceID, "azure-nic-resource-id", "", "[Azure] Route Server's NIC resource ID (format: /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Network/networkInterfaces/{nic})")

	return cmd
}
