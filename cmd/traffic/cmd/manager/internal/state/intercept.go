package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
	core "k8s.io/api/core/v1"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	typed "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/install"
	"github.com/telepresenceio/telepresence/v2/pkg/install/agent"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/version"
)

// PrepareIntercept ensures that the given request can be matched against the intercept configuration of
// the workload that it references. It returns a PreparedIntercept where all intercepted ports have been
// qualified with a service name and a service port name.
//
// The first step is to find the requested Workload and the agent Config for that workload. This step will
// create the initial ConfigMap for the namespace if it doesn't exist yet, and also generate the actual
// intercept config if it doesn't exist.
//
// The second step matches all ServicePortIdentifiers in the request to the intercepts of the agent Config
// and creates a resulting PreparedIntercept with a services array that has the same size and positions as
// the ServicePortIdentifiers in the request.
//
// It's expected that the client that makes the call will update any unqualified service port identifiers
// with the ones in the returned PreparedIntercept.
func (s *State) PrepareIntercept(ctx context.Context, cr *manager.CreateInterceptRequest) (*manager.PreparedIntercept, error) {
	interceptError := func(err error) (*manager.PreparedIntercept, error) {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return &manager.PreparedIntercept{Error: err.Error()}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	spec := cr.InterceptSpec
	wl, err := k8sapi.GetWorkload(ctx, spec.Agent, spec.Namespace, spec.WorkloadKind)
	if err != nil {
		return interceptError(err)
	}
	cmAPI := k8sapi.GetK8sInterface(ctx).CoreV1().ConfigMaps(spec.Namespace)
	cm, err := loadConfigMap(ctx, cmAPI, spec.Namespace)
	if err != nil {
		return interceptError(err)
	}
	ac, err := loadAgentConfig(ctx, cmAPI, cm, wl)
	if err != nil {
		return interceptError(err)
	}
	_, ics, err := findIntercepts(ac, spec)
	if err != nil {
		return interceptError(err)
	}
	svcs := make([]*manager.PreparedIntercept_Service, len(ics))
	for i, ic := range ics {
		svcs[i] = &manager.PreparedIntercept_Service{
			ServiceUid:      string(ic.ServiceUID),
			ServiceName:     ic.ServiceName,
			ServicePortName: ic.ServicePortName,
			ServicePort:     int32(ic.ServicePort),
		}
	}
	if err = s.waitForAgent(ctx, ac.AgentName, ac.Namespace); err != nil {
		return interceptError(err)
	}
	return &manager.PreparedIntercept{
		Namespace:    spec.Namespace,
		Services:     svcs,
		AgentImage:   ac.AgentImage,
		WorkloadKind: ac.WorkloadKind,
	}, nil
}

func loadConfigMap(ctx context.Context, cmAPI typed.ConfigMapInterface, namespace string) (*core.ConfigMap, error) {
	cm, err := cmAPI.Get(ctx, agent.ConfigMap, meta.GetOptions{})
	if err == nil {
		return cm, nil
	}
	if !errors2.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get ConfigMap %s.%s: %w", agent.ConfigMap, namespace, err)
	}
	cm, err = cmAPI.Create(ctx, &core.ConfigMap{
		TypeMeta: meta.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:      agent.ConfigMap,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       agent.ConfigMap,
				"app.kubernetes.io/created-by": "traffic-manager",
				"app.kubernetes.io/version":    strings.TrimPrefix(version.Version, "v"),
			},
		},
	}, meta.CreateOptions{})
	if err != nil {
		err = fmt.Errorf("failed to create ConfigMap %s.%s: %w", agent.ConfigMap, namespace, err)
	}
	return cm, err
}

func loadAgentConfig(ctx context.Context, cmAPI typed.ConfigMapInterface, cm *core.ConfigMap, wl k8sapi.Workload) (*agent.Config, error) {
	manuallyManaged, enabled, err := checkInterceptAnnotations(wl)
	if err != nil {
		return nil, err
	}
	if !(manuallyManaged || enabled) {
		return nil, fmt.Errorf("%s %s.%s is not interceptable", wl.GetKind(), wl.GetName(), wl.GetNamespace())
	}

	var ac *agent.Config
	if y, ok := cm.Data[wl.GetName()]; ok {
		if ac, err = unmarshalConfigMapEntry(y, wl.GetName(), wl.GetNamespace()); err != nil {
			return nil, err
		}
		if ac.Create {
			// This may happen if someone else is doing the initial intercept at the exact (well, more or less) same time
			if ac, err = waitForConfigMapUpdate(ctx, cmAPI, wl.GetName(), wl.GetNamespace()); err != nil {
				return nil, err
			}
		}
	} else {
		if manuallyManaged {
			return nil, fmt.Errorf(
				"annotation %s.%s/%s=true but workload has no corresponding entry in the %s ConfigMap",
				wl.GetName(), wl.GetNamespace(), install.ManualInjectAnnotation, agent.ConfigMap)
		}
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}
		cm.Data[wl.GetName()] = fmt.Sprintf("create: true\nworkloadKind: %s\nworkloadName: %s\nnamespace: %s",
			wl.GetKind(), wl.GetName(), wl.GetNamespace())
		if _, err := cmAPI.Update(ctx, cm, meta.UpdateOptions{}); err != nil {
			return nil, fmt.Errorf("failed update entry for %s in ConfigMap %s.%s: %w", wl.GetName(), agent.ConfigMap, wl.GetNamespace(), err)
		}
		if ac, err = waitForConfigMapUpdate(ctx, cmAPI, wl.GetName(), wl.GetNamespace()); err != nil {
			return nil, err
		}
	}
	return ac, nil
}

func checkInterceptAnnotations(wl k8sapi.Workload) (bool, bool, error) {
	pod := wl.GetPodTemplate()
	a := pod.Annotations
	if a == nil {
		return false, true, nil
	}

	webhookEnabled := true
	manuallyManaged := a[install.ManualInjectAnnotation] == "true"
	ia := a[install.InjectAnnotation]
	switch ia {
	case "":
		webhookEnabled = !manuallyManaged
	case "enabled":
		if manuallyManaged {
			return false, false, fmt.Errorf(
				"annotation %s.%s/%s=enabled cannot be combined with %s=true",
				wl.GetName(), wl.GetNamespace(), install.InjectAnnotation, install.ManualInjectAnnotation)
		}
	case "false", "disabled":
		webhookEnabled = false
	default:
		return false, false, fmt.Errorf(
			"%s is not a valid value for the %s.%s/%s annotation",
			ia, wl.GetName(), wl.GetNamespace(), install.ManualInjectAnnotation)
	}

	if !manuallyManaged {
		return false, webhookEnabled, nil
	}
	cns := pod.Spec.Containers
	var an *core.Container
	for i := range cns {
		cn := &cns[i]
		if cn.Name == agent.ContainerName {
			an = cn
			break
		}
	}
	if an == nil {
		return false, false, fmt.Errorf(
			"annotation %s.%s/%s=true but pod has no traffic-agent container",
			wl.GetName(), wl.GetNamespace(), install.ManualInjectAnnotation)
	}
	return false, true, nil
}

// Wait for the cluster's mutating webhook injector to do its magic. It will update the
// configMap once it's done.
func waitForConfigMapUpdate(ctx context.Context, cmAPI typed.ConfigMapInterface, agentName, namespace string) (*agent.Config, error) {
	wi, err := cmAPI.Watch(ctx, meta.SingleObject(meta.ObjectMeta{
		Name:      agent.ConfigMap,
		Namespace: namespace,
	}))
	if err != nil {
		return nil, fmt.Errorf("Watch of ConfigMap  %s failed: %w", agent.ConfigMap, ctx.Err())
	}
	defer wi.Stop()

	for {
		select {
		case <-ctx.Done():
			v := "canceled"
			c := codes.Canceled
			if ctx.Err() == context.DeadlineExceeded {
				v = "timed out"
				c = codes.DeadlineExceeded
			}
			return nil, status.Error(c, fmt.Sprintf("watch of ConfigMap %s[%s]: request %s", agent.ConfigMap, agentName, v))
		case ev, ok := <-wi.ResultChan():
			if !ok {
				return nil, status.Error(codes.Canceled, fmt.Sprintf("watch of ConfigMap  %s[%s]: channel closed", agent.ConfigMap, agentName))
			}
			if !(ev.Type == watch.Added || ev.Type == watch.Modified) {
				continue
			}
			if m, ok := ev.Object.(*core.ConfigMap); ok {
				if y, ok := m.Data[agentName]; ok {
					conf, ir := unmarshalConfigMapEntry(y, agentName, namespace)
					if ir != nil {
						return nil, ir
					}
					if !conf.Create {
						return conf, nil
					}
				}
			}
		}
	}
}

func (s *State) waitForAgent(ctx context.Context, name, namespace string) error {
	snapshotCh := s.WatchAgents(ctx, nil)
	for {
		select {
		case snapshot, ok := <-snapshotCh:
			if !ok {
				// The request has been canceled.
				return status.Error(codes.Canceled, fmt.Sprintf("channel closed while waiting for agent %s.%s to arrive", name, namespace))
			}
			for _, a := range snapshot.State {
				if a.Namespace == namespace && a.Name == name {
					return nil
				}
			}
		case <-ctx.Done():
			v := "canceled"
			c := codes.Canceled
			if ctx.Err() == context.DeadlineExceeded {
				v = "timed out"
				c = codes.DeadlineExceeded
			}
			return status.Error(c, fmt.Sprintf("request %s while waiting for agent %s.%s to arrive", v, name, namespace))
		}
	}
}

func unmarshalConfigMapEntry(y string, name, namespace string) (*agent.Config, error) {
	conf := agent.Config{}
	if err := yaml.Unmarshal([]byte(y), &conf); err != nil {
		return nil, fmt.Errorf("failed to parse entry for %s in ConfigMap %s.%s: %w", name, agent.ConfigMap, namespace, err)
	}
	return &conf, nil
}

func findUnused(cn *agent.Container, foundICs []*agent.Intercept) bool {
nextIC:
	for _, ic := range cn.Intercepts {
		for _, fis := range foundICs {
			if ic == fis {
				// already taken
				continue nextIC
			}
		}
		for i, fis := range foundICs {
			if fis == nil {
				foundICs[i] = ic
				return true
			}
		}
	}
	return false
}

// findIntercept finds the intercept configurations that matches the given InterceptSpec's ports
// The returned slice will have the same length as, and corresponding positions to, the spec's serviceInterceptIdentifier slice
func findIntercepts(ac *agent.Config, spec *manager.InterceptSpec) (foundCN *agent.Container, foundICs []*agent.Intercept, err error) {
	spis := spec.ServicePortIdentifiers
	foundICs = make([]*agent.Intercept, len(spis))

	unqualifiedCount := 0
nextSPI:
	for i, spi := range spis {
		if spi == "" {
			unqualifiedCount++
			continue
		}
		for _, cn := range ac.Containers {
			for _, ic := range cn.Intercepts {
				if !(spec.ServiceName == "" || spec.ServiceName == ic.ServiceName) {
					continue
				}
				if agent.IsInterceptFor(spi, ic) {
					foundICs[i] = ic
					if foundCN != nil && foundCN != cn {
						return nil, nil, errors.New("found multiple matching service ports that spans several containers.\n" +
							"Please specify the Services and/or Service ports you want to intercept " +
							"by passing the --service=<svc> and/or --port=<local:svcPortName> flag.")
					}
					foundCN = cn
					continue nextSPI
				}
			}
		}
		if spec.ServiceName != "" {
			return nil, nil, fmt.Errorf("unable to find intercept for service %q, port %s", spec.ServiceName, spi)
		}
		return nil, nil, fmt.Errorf("unable to find intercept for service port %s", spi)
	}
	switch unqualifiedCount {
	case 0:
		return foundCN, foundICs, err
	case 1:
		// An intercept spec may contain at maximum one intercept that isn't qualified. It will be assigned to the
		// first unused interface with a matching service name, either in the already found container, or in the
		// first container with intercepts.
		if foundCN != nil {
			// Has to be in the same container
			if findUnused(foundCN, foundICs) {
				unqualifiedCount--
			}
		} else {
			for _, cn := range ac.Containers {
				if findUnused(cn, foundICs) {
					foundCN = cn
					unqualifiedCount--
					break
				}
			}
		}
		if unqualifiedCount == 0 {
			return foundCN, foundICs, err
		}
		return nil, nil, fmt.Errorf("unable to find intercept for service %q", spec.ServiceName)
	default:
		return nil, nil, fmt.Errorf("at least %d service ports must be specified", len(spis)-1)
	}
}
