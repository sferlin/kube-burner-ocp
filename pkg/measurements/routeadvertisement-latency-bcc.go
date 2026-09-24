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
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	raLatencyBCCMeasurement          = "raLatencyBCCMeasurement"
	raLatencyBCCQuantilesMeasurement = "raLatencyBCCQuantilesMeasurement"
	// number of threads validating routes
	workerCount = 20
)

var (
	// Max timeout to wait for finishing work
	maxTimeout time.Duration = 1 * time.Minute
	// AWS region where the route server is deployed
	awsRegion string
	// AWS Route Server ID
	routeServerId string
	// AWS Route Server poll interval
	routesPollInterval time.Duration = 1 * time.Second

	supportedRaLatencyBCCJobTypes = []config.JobType{config.CreationJob, config.PatchJob}
)

type raMetricBCC struct {
	Timestamp  time.Time `json:"timestamp"`
	MetricName string    `json:"metricName"`
	UUID       string    `json:"uuid"`
	JobName    string    `json:"jobName,omitempty"`
	// route advertisement name
	Name     string `json:"routeAdvertisementName"`
	Metadata any    `json:"metadata,omitempty"`
	// list of cudn advertised by this route advertisement
	cudn []string
	// when an ra exports multiple cudn subnets, we measure latecny for each cudn. So belowLatency slice is ping latency for each cudn.
	Latency []float64 `json:"latency,omitempty"`
	// ping test latency. Note: ReadyLatency is p99, calculated on Latency
	MinReadyLatency int `json:"minReadyLatency"`
	MaxReadyLatency int `json:"maxReadyLatency"`
	P99ReadyLatency int `json:"p99readyLatency"`
	// AWS Route Server route detection latency
	AwsRouteServerRouteLatency    []float64 `json:"awsRouteServerRouteLatency,omitempty"`
	MaxAwsRouteServerRouteLatency int       `json:"maxAwsRouteServerRouteLatency,omitempty"`
	MinAwsRouteServerRouteLatency int       `json:"minAwsRouteServerRouteLatency,omitempty"`
	P99AwsRouteServerRouteLatency int       `json:"p99AwsRouteServerRouteLatency,omitempty"`
}

// type holds route detection and cund pod's ping timestamps
type detectedRoutes struct {
	// duplicate routes are not allowed, so routeTimestamp is not a slice
	// measure timestamp when a route (belonging to cudn's subnet) is first seen
	routeTimestamp time.Time
	// cudn subnet is exported as route. KB saw this route. Now KB pings the corresponding cudn's pod and stores success timestamp.
	pingTimestamps []time.Time
}

type raLatencyBCC struct {
	measurements.BaseMeasurement

	// list of cudn and their pods advertised by this route
	cudnSubnet map[string]cudnPods
	// timestamp when cudn is detected on external host and later events of ping tests
	cudnConnTimestamp sync.Map
	// how many routes verified, helpful for closing the worker threads
	verifiedRouteCount uint64
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

// Record RouteAdvertisement name and creation timestamp when routeadvertisement resource is detected by the API
func (r *raLatencyBCC) handleAdd(obj any) {
	var cudn []string
	ra := obj.(*unstructured.Unstructured)
	networkSelectors, found, err := unstructured.NestedSlice(ra.UnstructuredContent(), "spec", "networkSelectors")
	if err != nil {
		log.Error(err)
		return
	}
	if !found || len(networkSelectors) == 0 {
		log.Errorf("No networkSelectors found")
		return
	}
	networkSelector, ok := networkSelectors[0].(map[string]interface{})
	if !ok {
		log.Errorf("NetworkSelector doesn't exist")
		return
	}
	labels, found, err := unstructured.NestedStringMap(networkSelector, "clusterUserDefinedNetworkSelector", "networkSelector", "matchLabels")
	if err != nil {
		log.Error(err)
		return
	}
	if !found {
		log.Errorf("No labels found in networkSelector")
		return
	}

	ls := &metav1.LabelSelector{}
	err = metav1.Convert_Map_string_To_string_To_v1_LabelSelector(&labels, ls, nil)
	if err != nil {
		log.Error(err)
		return
	}
	selector, err := metav1.LabelSelectorAsSelector(ls)
	if err != nil {
		log.Error(err)
		return
	}
	listOptions := metav1.ListOptions{}
	listOptions.LabelSelector = selector.String()
	udns, err := r.connector.DynamicClient().Resource(cudnGVR).Namespace(metav1.NamespaceAll).List(context.TODO(), listOptions)
	if err != nil {
		log.Error(err)
		return
	}
	for _, udn := range udns.Items {
		cname, _, _ := unstructured.NestedString(udn.UnstructuredContent(), "metadata", "name")
		cudn = append(cudn, cname)
	}
	raName, _, _ := unstructured.NestedString(ra.UnstructuredContent(), "metadata", "name")
	ts, _, _ := unstructured.NestedString(ra.UnstructuredContent(), "metadata", "creationTimestamp")
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		log.Error(err)
		return
	}
	log.Debugf("RA %s discovered at: %v created at: %v", raName, time.Now().UTC(), t.UTC())
	r.Metrics.LoadOrStore(raName, raMetricBCC{
		Name:       raName,
		Timestamp:  t.UTC(),
		Latency:    []float64{},
		cudn:       cudn,
		MetricName: raLatencyBCCMeasurement,
		UUID:       r.Uuid,
		Metadata:   r.Metadata,
		JobName:    r.JobConfig.Name,
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
}

func (r *raLatencyBCC) observerWorker(routeServer *bccRouteServerObserver, routeCh chan<- newRoute, baselineReadCh chan<- struct{}) {
	defer r.wg.Done()

	ticker := time.NewTicker(routesPollInterval)
	defer ticker.Stop()

	baselineRead := false
	currentSubnets := make(map[string]bool)

	for {
		select {
		case <-ticker.C:
			routes, err := routeServer.getRoutes(context.Background())
			if err != nil {
				log.Warnf("BCC AWS Route Server query failed: %v", err)
				continue
			}

			fetchedSubnets := make(map[string]bool)
			for _, route := range routes {
				subnetString := aws.ToString(route.Prefix)
				fetchedSubnets[subnetString] = true
				if _, exists := currentSubnets[subnetString]; !exists {
					currentSubnets[subnetString] = true
					if baselineRead {
						// New route detected, send it to the worker channel
						log.Debugf("New route detected: %s", subnetString)
						routeCh <- newRoute{Dst: subnetString}
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

			if !baselineRead {
				baselineRead = true
				close(baselineReadCh)
			}
		case <-r.doneCh:
			return
		}
	}
}

/*
When KB creates RA CRDs, CUDN subnets are advertised and become observable from the AWS Route Server.

This go thread, reads the route(nothing but the subnet), retrieve the corresponding cudn (remember we already created a map "r.cudnSubnet" of subnet, sudn and pods mapping during start of measurements code) pods and pings them.

Note: we have only one pod per cudn subnet. So when a cudn subnet is detected, we ping only one pod (i.e the subnet's corresponding pod). So we don't need parallel executing of ping test.
*/

func (r *raLatencyBCC) worker(routeCh <-chan newRoute) {
	defer r.wg.Done()
	for {
		select {
		case update, ok := <-routeCh:
			if !ok {
				return
			}
			cudnpods, exists := r.cudnSubnet[update.Dst]
			if exists {
				// duplicate routes are not allowed, so new routeTimestamp should be added
				log.Debugf("Route: %s received for udn: %s at: %v", update.Dst, cudnpods.cudn, time.Now().UTC())
				val, _ := r.cudnConnTimestamp.LoadOrStore(cudnpods.cudn, detectedRoutes{
					routeTimestamp: time.Now().UTC(),
					pingTimestamps: []time.Time{}})
				if nlRouteVal, ok := val.(detectedRoutes); ok {
					pingSuccess := nlRouteVal.pingTimestamps
					for _, pod := range cudnpods.pods {
						for range pingAttempts {
							if err := pingAddress("", pod, exportPingerTimeoutMsec); err == nil {
								log.Debugf("Ping success to pod %s for the route: %s received for udn: %s at: %v", pod, update.Dst, cudnpods.cudn, time.Now().UTC())
								pingSuccess = append(pingSuccess, time.Now().UTC())
								break
							}
							time.Sleep(exportWaitBeforePingRetryMsec * time.Millisecond)
						}
					}
					nlRouteVal.pingTimestamps = pingSuccess
					atomic.AddUint64(&r.verifiedRouteCount, 1)
					r.cudnConnTimestamp.Store(cudnpods.cudn, nlRouteVal)
				}
			}
		case <-r.doneCh:
			return
		}
	}
}

/*
Start monitoring
1. Start a go thread that keeps polling the AWS Route Server for route changes. When a new route is detected, it is sent to the routeCh channel.
2. Starts workers, which read from the subscribed routeCh channel
3. register an informer for router advertisement resource creation events
*/
func (r *raLatencyBCC) startMonitoring() error {
	r.cudnConnTimestamp = sync.Map{}

	routeServer, err := newBCCRouteServerObserver(awsRegion, routeServerId)
	if err != nil {
		return err
	}

	// Start observer goroutine to poll AWS Route Server for route changes
	routeCh := make(chan newRoute, 1000)
	baselineReadCh := make(chan struct{})

	r.wg.Add(1)
	go r.observerWorker(routeServer, routeCh, baselineReadCh)
	<-baselineReadCh // wait for baseline read to complete before starting workers

	// Start worker goroutines
	for range workerCount {
		r.wg.Add(1)
		go r.worker(routeCh)
	}

	log.Infof("Creating Router Advertisement latency watcher for %s", r.JobConfig.Name)
	connector, err := k8sconnector.NewK8SConnector(r.RestConfig)
	if err != nil {
		log.Error(err)
		return err
	}
	r.connector = connector
	raFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(r.connector.DynamicClient(), time.Minute, metav1.NamespaceAll, nil)
	raInformer := raFactory.ForResource(raGVR).Informer()
	raInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: r.handleAdd,
	})

	stopCh := make(chan struct{})
	raFactory.Start(stopCh)
	raFactory.WaitForCacheSync(stopCh)
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

	// cudn pods which will be pinged
	// Maintain a list of cudn subnets and their pods
	r.cudnSubnet, err = getPods(r.ClientSet, r.Uuid)
	if err != nil {
		return err
	}
	for subnetString, cudnpods := range r.cudnSubnet {
		// Example: CUDN: cudn-0, Subnet: 40.0.3.0/24, Pods: [40.0.3.3]
		log.Debugf("CUDN: %s, Subnet: %s, Pods: %v", cudnpods.cudn, subnetString, cudnpods.pods)
	}

	if err = r.startMonitoring(); err != nil {
		return err
	}
	return nil
}

func (r *raLatencyBCC) Collect(measurementWg *sync.WaitGroup) {
	defer measurementWg.Done()
}

/*
Periodically check if work completed by measuring how many routes validated.
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
			count = atomic.LoadUint64(&r.verifiedRouteCount)
			log.Debugf("count %v , desiredCount %v", count, desiredCount)
			if count >= desiredCount {
				log.Debugf("Desired count reached, signaling stop.")
				// Give additional 10 seconds for threads to finish (ping test after detecting routes)
				time.Sleep(10 * time.Second)
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

	// Wait till all CUDNs exported using RAs i.e wait for export scenario validation
	// We are assuming all CUDN's will be exported using RAs
	desiredCount := uint64(len(r.cudnSubnet))
	r.waitForCompletion(desiredCount)
	// stop workers
	close(r.doneCh)

	r.wg.Wait()

	return r.StopMeasurement(r.normalizeMetrics, r.getLatency)
}

func (r *raLatencyBCC) normalizeMetrics() float64 {
	r.Metrics.Range(func(key, value any) bool {
		m := value.(raMetricBCC)

		for _, udn := range m.cudn {
			val, exists := r.cudnConnTimestamp.Load(udn)
			if exists {
				routeVal := val.(detectedRoutes)
				for _, ts := range routeVal.pingTimestamps {
					m.Latency = append(m.Latency, float64(ts.Sub(m.Timestamp).Milliseconds()))
				}
				m.AwsRouteServerRouteLatency = append(m.AwsRouteServerRouteLatency, float64(routeVal.routeTimestamp.Sub(m.Timestamp).Milliseconds()))
			}
		}
		// Index ping latency
		latencySummary := metrics.NewLatencySummary(m.Latency, m.Name)
		log.Tracef("%s: 50th: %d 95th: %d 99th: %d min: %d max: %d avg: %d\n", m.Name, latencySummary.P50, latencySummary.P95, latencySummary.P99, latencySummary.Min, latencySummary.Max, latencySummary.Avg)

		m.MinReadyLatency = latencySummary.Min
		m.MaxReadyLatency = latencySummary.Max
		m.P99ReadyLatency = latencySummary.P99

		// Index AWS Route Server route detection latency
		nrLatencySummary := metrics.NewLatencySummary(m.AwsRouteServerRouteLatency, m.Name)
		log.Tracef("%s: 50th: %d 95th: %d 99th: %d min: %d max: %d avg: %d\n", m.Name, nrLatencySummary.P50, nrLatencySummary.P95, nrLatencySummary.P99, nrLatencySummary.Min, nrLatencySummary.Max, nrLatencySummary.Avg)

		m.MinAwsRouteServerRouteLatency = nrLatencySummary.Min
		m.MaxAwsRouteServerRouteLatency = nrLatencySummary.Max
		m.P99AwsRouteServerRouteLatency = nrLatencySummary.P99

		r.NormLatencies = append(r.NormLatencies, m)
		return true
	})
	return 0
}

func (r *raLatencyBCC) getLatency(normLatency any) map[string]float64 {
	raMetric := normLatency.(raMetricBCC)
	return map[string]float64{
		"MinReadyLatency":               float64(raMetric.MinReadyLatency),
		"MaxReadyLatency":               float64(raMetric.MaxReadyLatency),
		"P99ReadyLatency":               float64(raMetric.P99ReadyLatency),
		"MinAwsRouteServerRouteLatency": float64(raMetric.MinAwsRouteServerRouteLatency),
		"MaxAwsRouteServerRouteLatency": float64(raMetric.MaxAwsRouteServerRouteLatency),
		"P99AwsRouteServerRouteLatency": float64(raMetric.P99AwsRouteServerRouteLatency),
	}
}

func (r *raLatencyBCC) IsCompatible() bool {
	return slices.Contains(supportedRaLatencyBCCJobTypes, r.JobConfig.JobType)
}
