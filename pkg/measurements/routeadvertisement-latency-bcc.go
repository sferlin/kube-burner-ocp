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
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
/*type netlinkRoutes struct {
	// linux doesn't allow adding duplicate routes, so routeTimestamp is not a slice
	// measure timestamp when a route (belonging to cudn's subnet) detected by the kernel
	routeTimestamp time.Time
	// cudn subnet is exported as route. KB detected this route in its host. Now KB pings the corresponding cudn's pod and stores success timestamp.
	pingTimestamps []time.Time
}*/

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

// unlike default pod network, when a pod is created on udn network, pod ip address is retrieved from pod annotations.
// we create list of cudn subnet and pod ip mappings. CUdn subnet is considered as a route exported to outside the cluster. When KB wants to ping test the cudn, it pings cudn's pods.
func (r *raLatencyBCC) getPods() error {
	var err error
	listOptions := metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", config.KubeBurnerLabelUUID, r.Uuid)}
	nsList, err := r.ClientSet.CoreV1().Namespaces().List(context.TODO(), listOptions)
	if err != nil {
		log.Errorf("Error listing namespaces: %v", err)
		return err
	}
	for _, ns := range nsList.Items {
		podList, err := r.ClientSet.CoreV1().Pods(ns.Name).List(context.TODO(), listOptions)
		if err != nil {
			log.Errorf("Error listing pods in namespace %s: %v", ns.Name, err)
			return err
		}
		for _, pod := range podList.Items {
			podNetworks := make(map[string]podAnnotation)
			ovnAnnotation, ok := pod.Annotations["k8s.ovn.org/pod-networks"]
			if ok {
				if err := json.Unmarshal([]byte(ovnAnnotation), &podNetworks); err != nil {
					log.Errorf("failed to unmarshal ovn pod annotation  %v", err)
					continue
				}
				for pnet, val := range podNetworks {
					if pnet != "default" {
						var udn string
						parts := strings.Split(pnet, "/")
						if len(parts) == 2 {
							_, udn = parts[0], parts[1]
						} else {
							log.Debugf("Invalid input format")
							continue
						}
						ipAddr, subnet, err := net.ParseCIDR(val.IP)
						if err != nil {
							log.Debugf("Unable to get CIDR for IP")
							continue
						}
						subnetString := subnet.String()
						ipAddrString := ipAddr.String()
						cudnpods, exists := r.cudnSubnet[subnetString]
						if exists {
							cudnpods.pods = append(cudnpods.pods, ipAddrString)
							r.cudnSubnet[subnetString] = cudnpods
						} else {
							r.cudnSubnet[subnetString] = cudnPods{
								cudn: udn,
								pods: []string{ipAddrString},
							}
						}
					}
				}
			}
		}
	}
	return nil
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

/*
When KB creates RA CRDs, CUDN subnets are advertised and become observable from the AWS Route Server.

This go thread, reads the route(nothing but the subnet), retrieve the corresponding cudn (remember we already created a map "r.cudnSubnet" of subnet, sudn and pods mapping during start of measurements code) pods and pings them.

Note: we have only one pod per cudn subnet. So when a cudn subnet is detected, we ping only one pod (i.e the subnet's corresponding pod). So we don't need parallel executing of ping test.
*/

func (r *raLatencyBCC) worker() {
	r.wg.Done()
	for {
		select {
		/*case update, ok := <-r.routeCh:
		if !ok {
			return
		}
		if update.Type == unix.RTM_NEWROUTE {
			cudnpods, exists := r.cudnSubnet[update.Dst.String()]
			if exists {
				// linux doesn't allow adding duplicate routes, so new routeTimestamp should be added
				log.Debugf("Netlink route: %s received for udn: %s at: %v", update.Dst.String(), cudnpods.cudn, time.Now().UTC())
				val, _ := r.cudnConnTimestamp.LoadOrStore(cudnpods.cudn, netlinkRoutes{
					routeTimestamp: time.Now().UTC(),
					pingTimestamps: []time.Time{}})
				if nlRouteVal, ok := val.(netlinkRoutes); ok {
					pingSuccess := nlRouteVal.pingTimestamps
					for _, pod := range cudnpods.pods {
						for range pingAttempts {
							if err := pingAddress("", pod, exportPingerTimeoutMsec); err == nil {
								log.Debugf("Ping success to pod %s for the Netlink route: %s received for udn: %s at: %v", pod, update.Dst.String(), cudnpods.cudn, time.Now().UTC())
								pingSuccess = append(pingSuccess, time.Now().UTC())
								break
							}
							time.Sleep(exportWaitBeforePingRetryMsec * time.Millisecond)
						}
					}
					nlRouteVal.pingTimestamps = pingSuccess
					atomic.AddUint64(&r.verifiedExportRouteCount, 1)
					r.cudnConnTimestamp.Store(cudnpods.cudn, nlRouteVal)
				}
			}
		}*/
		case <-r.doneCh:
			break
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
	var err error
	r.cudnConnTimestamp = sync.Map{}

	// TODO: create r.routeCh and a go thread that keeps polling the AWS Route Server for route changes. When a new route is detected, it is sent to the routeCh channel.

	// Start worker goroutines
	for range workerCount {
		r.wg.Add(1)
		go r.worker()
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
	r.cudnSubnet = make(map[string]cudnPods)

	// Maintain a list of cudn subnets and their pods
	r.getPods()

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

		/*for _, udn := range m.cudn {
			_, exists := r.cudnConnTimestamp.Load(udn)
			if exists {
				nlRouteVal := val.(netlinkRoutes)
				for _, ts := range nlRouteVal.pingTimestamps {
					m.Latency = append(m.Latency, float64(ts.Sub(m.Timestamp).Milliseconds()))
				}
				m.NetlinkRouteLatency = append(m.NetlinkRouteLatency, float64(nlRouteVal.routeTimestamp.Sub(m.Timestamp).Milliseconds()))
			}
		}*/
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
