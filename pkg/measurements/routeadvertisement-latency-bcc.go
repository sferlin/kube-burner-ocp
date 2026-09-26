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

package measurements

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	k8sconnector "github.com/cloud-bulldozer/go-commons/v2/k8s-connector"
	"github.com/kube-burner/kube-burner/v2/pkg/config"
	"github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/measurements/metrics"
	"github.com/kube-burner/kube-burner/v2/pkg/measurements/types"
	"github.com/kube-burner/kube-burner/v2/pkg/util/fileutils"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	raLatencyBCCMeasurement          = "raLatencyBCCMeasurement"
	raLatencyBCCQuantilesMeasurement = "raLatencyBCCQuantilesMeasurement"
)

var (
	// Max timeout to wait for finishing work
	maxTimeout time.Duration = 1 * time.Minute
	// AWS region where the route server is deployed
	awsRegion string
	// AWS Route Server ID
	routeServerId string
	// AWS Route Server poll interval
	routesPollInterval time.Duration = 250 * time.Millisecond

	supportedRaLatencyBCCJobTypes = []config.JobType{config.CreationJob, config.PatchJob}

	// BGPRouting GVR for BCC variant
	bgpRoutingGVR = schema.GroupVersionResource{
		Group:    "networking.openshift.io",
		Version:  "v1beta1",
		Resource: "bgproutings",
	}
)

type raMetricBCC struct {
	Timestamp  time.Time `json:"timestamp"`
	MetricName string    `json:"metricName"`
	UUID       string    `json:"uuid"`
	JobName    string    `json:"jobName,omitempty"`
	// BGPRouting name
	Name     string `json:"bgpRoutingName"`
	Metadata any    `json:"metadata,omitempty"`
	// Subnets advertised by this BGPRouting
	Subnets []string `json:"subnets"`
	// AWS Route Server route detection latency (one per subnet)
	AwsRouteServerRouteLatency    []float64 `json:"awsRouteServerRouteLatency,omitempty"`
	MaxAwsRouteServerRouteLatency int       `json:"maxAwsRouteServerRouteLatency,omitempty"`
	MinAwsRouteServerRouteLatency int       `json:"minAwsRouteServerRouteLatency,omitempty"`
	P99AwsRouteServerRouteLatency int       `json:"p99AwsRouteServerRouteLatency,omitempty"`
	AvgAwsRouteServerRouteLatency int       `json:"avgAwsRouteServerRouteLatency,omitempty"`
}

// type holds route detection timestamp for each subnet
type detectedRoute struct {
	// Timestamp when route was first seen in AWS Route Server
	routeTimestamp time.Time
}

type raLatencyBCC struct {
	measurements.BaseMeasurement

	// timestamp when route is detected in AWS Route Server (key: subnet string, value: detectedRoute)
	routeTimestamps sync.Map
	// how many routes detected, helpful for closing the worker threads
	detectedRouteCount uint64
	// channel to notify workers
	doneCh    chan struct{}
	wg        sync.WaitGroup
	connector k8sconnector.K8SConnector
}

type raLatencyBCCMeasurementFactory struct {
	measurements.BaseMeasurementFactory
}

func NewRaLatencyBCCMeasurementFactory(configSpec config.Spec, measurement types.Measurement, metadata map[string]any, labelSelector string) (measurements.MeasurementFactory, error) {
	return raLatencyBCCMeasurementFactory{
		measurements.NewBaseMeasurementFactory(configSpec, measurement, metadata, labelSelector),
	}, nil
}

func (plmf raLatencyBCCMeasurementFactory) NewMeasurement(jobConfig *config.Job, clientSet kubernetes.Interface, restConfig *rest.Config, embedCfg *fileutils.EmbedConfiguration) measurements.Measurement {
	return &raLatencyBCC{
		BaseMeasurement: plmf.NewBaseLatency(jobConfig, clientSet, restConfig, raLatencyBCCMeasurement, raLatencyBCCQuantilesMeasurement, embedCfg),
	}
}

// Record BGPRouting name and creation timestamp when BGPRouting resource is detected by the API
func (r *raLatencyBCC) handleAdd(obj any) {
	bgpRouting := obj.(*unstructured.Unstructured)

	// Extract BGPRouting name
	bgpRoutingName, _, _ := unstructured.NestedString(bgpRouting.UnstructuredContent(), "metadata", "name")

	// Extract subnets from spec.network.subnets
	subnetsRaw, found, err := unstructured.NestedStringSlice(bgpRouting.UnstructuredContent(), "spec", "network", "subnets")
	if err != nil {
		log.Errorf("Error extracting subnets from BGPRouting %s: %v", bgpRoutingName, err)
		return
	}
	if !found || len(subnetsRaw) == 0 {
		log.Errorf("No subnets found in BGPRouting %s spec.network.subnets", bgpRoutingName)
		return
	}

	// Extract creation timestamp
	ts, _, _ := unstructured.NestedString(bgpRouting.UnstructuredContent(), "metadata", "creationTimestamp")
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		log.Errorf("Error parsing timestamp for BGPRouting %s: %v", bgpRoutingName, err)
		return
	}

	log.Debugf("BGPRouting %s (Subnets: %v) discovered at: %v created at: %v", bgpRoutingName, subnetsRaw, time.Now().UTC(), t.UTC())

	// Store metric
	r.Metrics.LoadOrStore(bgpRoutingName, raMetricBCC{
		Name:                       bgpRoutingName,
		Timestamp:                  t.UTC(),
		Subnets:                    subnetsRaw,
		AwsRouteServerRouteLatency: []float64{},
		MetricName:                 raLatencyBCCMeasurement,
		UUID:                       r.Uuid,
		Metadata:                   r.Metadata,
		JobName:                    r.JobConfig.Name,
	})
}

type bccRouteServerObserver struct {
	client        *ec2.Client
	routeServerID string
}

func newBCCRouteServerObserver(region, routeServerID string) (*bccRouteServerObserver, error) {
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

	return &bccRouteServerObserver{
		client:        ec2.NewFromConfig(cfg),
		routeServerID: routeServerID,
	}, nil
}

func (o *bccRouteServerObserver) getRoutes(ctx context.Context) ([]ec2types.RouteServerRoute, error) {
	var routes []ec2types.RouteServerRoute
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

		routes = append(routes, output.Routes...)

		if output.NextToken == nil || aws.ToString(output.NextToken) == "" {
			break
		}

		nextToken = output.NextToken
	}

	return routes, nil
}

type newRoute struct {
	Dst string
	Ts  time.Time
}

func (r *raLatencyBCC) observerWorker(routeServer *bccRouteServerObserver, routeCh chan<- newRoute, baselineReadCh chan<- struct{}) {
	defer r.wg.Done()

	baselineRead := false
	currentSubnets := make(map[string]bool)
	read := func() {
		routes, err := routeServer.getRoutes(context.Background())
		if err != nil {
			log.Warnf("BCC AWS Route Server query failed: %v", err)
			return
		}

		now := time.Now().UTC()
		fetchedSubnets := make(map[string]bool)
		for _, route := range routes {
			subnetString := aws.ToString(route.Prefix)
			fetchedSubnets[subnetString] = true
			if _, exists := currentSubnets[subnetString]; !exists {
				currentSubnets[subnetString] = true
				if baselineRead {
					// New route detected, send it to the worker channel
					log.Debugf("New route detected: %s", subnetString)
					routeCh <- newRoute{Dst: subnetString, Ts: now}
				} else {
					log.Debugf("Baseline route detected: %s", subnetString)
				}
			}
		}

		// Remove subnets that are no longer present in the fetched routes
		for subnet := range currentSubnets {
			if !fetchedSubnets[subnet] {
				delete(currentSubnets, subnet)
				log.Debugf("Route removed: %s", subnet)
			}
		}
	}

	read() // Initial read to establish baseline
	baselineRead = true
	log.Debugf("Baseline read complete, monitoring for new routes...")
	close(baselineReadCh)

	ticker := time.NewTicker(routesPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			read()
		case <-r.doneCh:
			return
		}
	}
}

/*
When BGPRouting CRs are created, routes are advertised and become observable from the AWS Route Server.
This worker records the timestamp when each route is detected.
*/
func (r *raLatencyBCC) worker(routeCh <-chan newRoute) {
	defer r.wg.Done()
	for {
		select {
		case update, ok := <-routeCh:
			if !ok {
				return
			}
			log.Debugf("Route: %s detected in AWS Route Server at: %v", update.Dst, update.Ts)
			r.routeTimestamps.LoadOrStore(update.Dst, detectedRoute{
				routeTimestamp: update.Ts,
			})
			atomic.AddUint64(&r.detectedRouteCount, 1)
		case <-r.doneCh:
			return
		}
	}
}

/*
Start monitoring
1. Start a go thread that keeps polling the AWS Route Server for route changes. When a new route is detected, it is sent to the routeCh channel.
2. Starts workers, which read from the subscribed routeCh channel
3. Register an informer for BGPRouting resource creation events
*/
func (r *raLatencyBCC) startMonitoring() error {
	r.routeTimestamps = sync.Map{}

	routeServer, err := newBCCRouteServerObserver(awsRegion, routeServerId)
	if err != nil {
		log.Errorf("Failed to create route observer: %v", err)
		return err
	}

	// Start observer goroutine to poll AWS Route Server for route changes
	routeCh := make(chan newRoute, 1000)
	baselineReadCh := make(chan struct{})

	r.wg.Add(1)
	go r.observerWorker(routeServer, routeCh, baselineReadCh)
	<-baselineReadCh // wait for baseline read to complete before starting workers

	// Start worker goroutine that may in the future do more than just record timestamps, e.g. validate routes in AWS Route Server
	// (and then we could have more than one worker)
	r.wg.Add(1)
	go r.worker(routeCh)

	log.Infof("Creating BGPRouting latency watcher for %s", r.JobConfig.Name)
	connector, err := k8sconnector.NewK8SConnector(r.RestConfig)
	if err != nil {
		log.Error(err)
		return err
	}
	r.connector = connector
	bgpRoutingFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(r.connector.DynamicClient(), time.Minute, metav1.NamespaceAll, nil)
	bgpRoutingInformer := bgpRoutingFactory.ForResource(bgpRoutingGVR).Informer()
	bgpRoutingInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: r.handleAdd,
	})

	stopCh := make(chan struct{})
	bgpRoutingFactory.Start(stopCh)
	bgpRoutingFactory.WaitForCacheSync(stopCh)
	return nil
}

// Read input variables from job templates
func (r *raLatencyBCC) setInputVars() {
	var err error
	for _, obj := range r.JobConfig.Objects {
		if val, ok := obj.InputVars["maxTimeout"]; ok {
			maxTimeout, err = time.ParseDuration(val.(string))
			if err != nil {
				log.Errorf("Failure parsing maxTimeout: %v", err)
			}
		}
		if val, ok := obj.InputVars["awsRegion"]; ok {
			awsRegion = val.(string)
		}
		if val, ok := obj.InputVars["routeServerId"]; ok {
			routeServerId = val.(string)
		}
	}
}

// start raLatency measurement
func (r *raLatencyBCC) Start(measurementWg *sync.WaitGroup) error {
	// Reset latency slices, required in multi-job benchmarks
	var err error
	r.LatencyQuantiles, r.NormLatencies = nil, nil
	r.Metrics = sync.Map{}

	defer measurementWg.Done()

	if r.JobConfig.SkipIndexing {
		return nil
	}
	r.setInputVars()

	// channel to notify workers to exit
	r.doneCh = make(chan struct{})

	if err = r.startMonitoring(); err != nil {
		return err
	}
	return nil
}

func (r *raLatencyBCC) Collect(measurementWg *sync.WaitGroup) {
	defer measurementWg.Done()
}

/*
Periodically check if work completed by measuring how many routes detected.
Exit if we hit maxTimeout though work might not have completed. We later notify all worker threads to return in this case.
*/
func (r *raLatencyBCC) waitForCompletion(desiredCount uint64) {
	var count uint64
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	timeoutTimer := time.NewTimer(maxTimeout)
	defer timeoutTimer.Stop()

	for {
		select {
		case <-ticker.C:
			count = atomic.LoadUint64(&r.detectedRouteCount)
			log.Debugf("count %v , desiredCount %v", count, desiredCount)
			if count >= desiredCount {
				log.Debugf("Desired count reached, signaling stop.")
				return
			}
		case <-timeoutTimer.C:
			log.Debugf("Timeout reached, signaling stop.")
			return
		}
	}
}

// Stop stops raLatency measurement
func (r *raLatencyBCC) Stop() error {
	if r.JobConfig.SkipIndexing {
		return nil
	}

	// Count total number of subnets advertised across all BGPRouting CRs
	var desiredCount uint64
	r.Metrics.Range(func(key, value any) bool {
		m := value.(raMetricBCC)
		desiredCount += uint64(len(m.Subnets))
		return true
	})

	log.Infof("Waiting for %d routes to be detected in AWS Route Server", desiredCount)
	r.waitForCompletion(desiredCount)

	// Stop workers
	close(r.doneCh)
	r.wg.Wait()

	return r.StopMeasurement(r.normalizeMetrics, r.getLatency)
}

func (r *raLatencyBCC) normalizeMetrics() float64 {
	r.Metrics.Range(func(key, value any) bool {
		m := value.(raMetricBCC)

		// For each subnet advertised by this BGPRouting, check if we detected it in AWS
		for _, subnet := range m.Subnets {
			val, exists := r.routeTimestamps.Load(subnet)
			if exists {
				routeVal := val.(detectedRoute)
				latencyMs := float64(routeVal.routeTimestamp.Sub(m.Timestamp).Milliseconds())
				m.AwsRouteServerRouteLatency = append(m.AwsRouteServerRouteLatency, latencyMs)
				log.Debugf("BGPRouting %s advertised subnet %s detected in AWS Route Server at: %v, latency: %f ms", m.Name, subnet, routeVal.routeTimestamp, latencyMs)
			} else {
				log.Warnf("BGPRouting %s advertised subnet %s but it was never detected in AWS Route Server", m.Name, subnet)
			}
		}

		// Calculate latency statistics
		if len(m.AwsRouteServerRouteLatency) > 0 {
			summary := metrics.NewLatencySummary(m.AwsRouteServerRouteLatency, m.Name)
			log.Infof("BGPRouting %s AWS Route Server latency - Min: %dms, Max: %dms, Avg: %dms, P99: %dms",
				m.Name, summary.Min, summary.Max, summary.Avg, summary.P99)

			m.MinAwsRouteServerRouteLatency = summary.Min
			m.MaxAwsRouteServerRouteLatency = summary.Max
			m.AvgAwsRouteServerRouteLatency = summary.Avg
			m.P99AwsRouteServerRouteLatency = summary.P99
		}

		r.NormLatencies = append(r.NormLatencies, m)
		return true
	})
	return 0
}

func (r *raLatencyBCC) getLatency(normLatency any) map[string]float64 {
	raMetric := normLatency.(raMetricBCC)
	return map[string]float64{
		"MinAwsRouteServerRouteLatency": float64(raMetric.MinAwsRouteServerRouteLatency),
		"MaxAwsRouteServerRouteLatency": float64(raMetric.MaxAwsRouteServerRouteLatency),
		"AvgAwsRouteServerRouteLatency": float64(raMetric.AvgAwsRouteServerRouteLatency),
		"P99AwsRouteServerRouteLatency": float64(raMetric.P99AwsRouteServerRouteLatency),
	}
}

func (r *raLatencyBCC) IsCompatible() bool {
	return slices.Contains(supportedRaLatencyBCCJobTypes, r.JobConfig.JobType)
}
