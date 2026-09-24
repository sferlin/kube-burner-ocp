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
	"fmt"
	"os"

	kubeburnermeasurements "github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/workloads"
	"github.com/spf13/cobra"

	"github.com/kube-burner/kube-burner-ocp/pkg/measurements"
)

var additionalMeasurementFactoryMapBCC = map[string]kubeburnermeasurements.NewMeasurementFactory{
	"raLatencyBCC": measurements.NewRaLatencyBCCMeasurementFactory,
}

// NewUdnBgpBCC creates the BGP Cloud Connector variant of the udn-bgp workload.
// It currently only supports AWS VPC Route Server and watches it for route changes.
func NewUdnBgpBCC(wh *workloads.WorkloadHelper, variant string) *cobra.Command {
	var iterations, namespacePerCudn, cidrsPerCudn int
	var enableVm, layer2 bool
	var metricsProfiles []string
	var awsRegion string
	var routeServerID string
	var rc int
	cmd := &cobra.Command{
		Use:   variant,
		Short: fmt.Sprintf("Runs %v workload", variant),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if cidrsPerCudn < 1 {
				return fmt.Errorf("--cidrs-per-cudn must be >= 1, got %d", cidrsPerCudn)
			}
			if iterations%namespacePerCudn != 0 {
				return fmt.Errorf("--iterations (%d) must be divisible by --namespaces-per-cudn (%d)", iterations, namespacePerCudn)
			}
			// cudn.yml computes firstOctet = (cidrIndex/255)+40; max valid cidrIndex is 55079 (firstOctet=255).
			// Total CIDRs = (iterations/namespacesPerCudn)*cidrsPerCudn must not exceed 55080.
			if totalCIDRs := (iterations / namespacePerCudn) * cidrsPerCudn; totalCIDRs > 55080 {
				return fmt.Errorf("--iterations/--namespaces-per-cudn * --cidrs-per-cudn yields %d total CIDRs, exceeding the maximum of 55080 (would produce an invalid first IP octet > 255)", totalCIDRs)
			}
			if routeServerID == "" {
				return fmt.Errorf("--route-server-id is required")
			}
			if awsRegion == "" {
				return fmt.Errorf("--aws-region is required")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			setMetrics(cmd, metricsProfiles)
			AdditionalVars["JOB_ITERATIONS"] = iterations
			AdditionalVars["NAMESPACES_PER_CUDN"] = namespacePerCudn
			AdditionalVars["CIDRS_PER_CUDN"] = cidrsPerCudn
			AdditionalVars["ENABLE_VM"] = enableVm
			AdditionalVars["LAYER2"] = layer2
			AdditionalVars["AWS_REGION"] = awsRegion
			AdditionalVars["ROUTE_SERVER_ID"] = routeServerID
			wh.SetMeasurements(additionalMeasurementFactoryMapBCC)
			rc = RunWorkload(cmd, wh, cmd.Name()+".yml")
		},
		PostRun: func(cmd *cobra.Command, args []string) {
			os.Exit(rc)
		},
	}
	cmd.Flags().IntVar(&iterations, "iterations", 10, fmt.Sprintf("%v iterations", variant))
	cmd.Flags().BoolVar(&enableVm, "vm", false, "Deploy a VM for the test instead of a pod")
	cmd.Flags().BoolVar(&layer2, "layer2", false, "Use Layer2 topology for CUDNs instead of Layer3")
	cmd.Flags().IntVar(&namespacePerCudn, "namespaces-per-cudn", 1, "Number of namespaces sharing the same cluster UDN")
	cmd.Flags().IntVar(&cidrsPerCudn, "cidrs-per-cudn", 1, "Number of CIDRs per CUDN")
	cmd.Flags().StringVar(&awsRegion, "aws-region", "", "AWS region containing the VPC Route Server")
	cmd.Flags().StringVar(&routeServerID, "route-server-id", "", "AWS VPC Route Server ID")
	cmd.Flags().StringSliceVar(&metricsProfiles, "metrics-profile", []string{"metrics.yml"}, "Comma separated list of metrics profiles to use")
	cmd.MarkFlagRequired("iterations")
	cmd.MarkFlagRequired("aws-region")
	cmd.MarkFlagRequired("route-server-id")
	return cmd
}
