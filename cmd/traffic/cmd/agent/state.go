package agent

import (
	"context"
	"fmt"
	"net/http"

	"github.com/blang/semver"

	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/forwarder"
	"github.com/telepresenceio/telepresence/v2/pkg/install/agent"
	"github.com/telepresenceio/telepresence/v2/pkg/restapi"
)

type State interface {
	HandleIntercepts(ctx context.Context, cepts []*manager.InterceptInfo) []*manager.ReviewInterceptRequest
	SetManager(sessionInfo *manager.SessionInfo, manager manager.ManagerClient, version semver.Version)
	AgentState() restapi.AgentState
}

type SimpleState interface {
	State
	AddIntercept([]*forwarder.Forwarder, *agent.Container, map[string]string)
}

// State of the Traffic Agent.
type state struct {
	managerHost     string
	namespace       string
	podIP           string
	sftpPort        int32
	interceptStates []*interceptState
}

type interceptState struct {
	state      *state
	config     *agent.Container
	forwarders []*forwarder.Forwarder
	mountPoint string
	env        map[string]string
	chosenID   string
}

func NewState(managerHost, namespace, podIP string, sftpPort int32) SimpleState {
	return &state{
		managerHost: managerHost,
		namespace:   namespace,
		podIP:       podIP,
		sftpPort:    sftpPort,
	}
}

func (s *state) AddIntercept(forwarders []*forwarder.Forwarder, config *agent.Container, env map[string]string) {
	s.interceptStates = append(s.interceptStates, newInterceptState(s, forwarders, config, env))
}

func (s *state) AgentState() restapi.AgentState {
	return s
}

func (s *state) InterceptInfo(ctx context.Context, callerID, path string, headers http.Header) (*restapi.InterceptInfo, error) {
	for _, is := range s.interceptStates {
		ii, err := is.interceptInfo(ctx, callerID, path, headers)
		if err != nil {
			return nil, err
		}
		if ii.Intercepted {
			return ii, nil
		}
	}
	return &restapi.InterceptInfo{}, nil
}

func (s *state) SetManager(sessionInfo *manager.SessionInfo, manager manager.ManagerClient, version semver.Version) {
	for _, is := range s.interceptStates {
		is.setManager(sessionInfo, manager, version)
	}
}

func (s *state) HandleIntercepts(ctx context.Context, iis []*manager.InterceptInfo) []*manager.ReviewInterceptRequest {
	var rs []*manager.ReviewInterceptRequest
	for _, ist := range s.interceptStates {
		miis := make([]*manager.InterceptInfo, 0, len(iis))
		for _, is := range ist.config.Intercepts {
			for _, ii := range iis {
				if agent.SpecMatchesIntercept(ii.Spec, is) >= 0 {
					dlog.Debugf(ctx, "intercept id %s svc=%q, svcPort=%q matches config svc=%q, svcPort=%d",
						ii.Id, ii.Spec.ServiceName, ii.Spec.ServicePortIdentifier, is.ServiceName, is.ServicePort)
					miis = append(miis, ii)
				}
			}
		}
		rs = append(rs, ist.handleIntercepts(ctx, miis)...)
	}
	return rs
}

func (s *interceptState) setManager(sessionInfo *manager.SessionInfo, manager manager.ManagerClient, version semver.Version) {
	for _, fwd := range s.forwarders {
		fwd.SetManager(sessionInfo, manager, version)
	}
}

func (s *interceptState) interceptInfo(ctx context.Context, callerID, path string, headers http.Header) (*restapi.InterceptInfo, error) {
	// The OSS agent is either intercepting or it isn't. There's no way to tell what it is that's being intercepted.
	return s.forwarders[0].InterceptInfo(), nil
}

func newInterceptState(s *state, forwarders []*forwarder.Forwarder, config *agent.Container, env map[string]string) *interceptState {
	return &interceptState{
		state:      s,
		config:     config,
		forwarders: forwarders,
		env:        env,
	}
}

func (s *interceptState) handleIntercepts(ctx context.Context, cepts []*manager.InterceptInfo) []*manager.ReviewInterceptRequest {
	var chosenIntercept, activeIntercept *manager.InterceptInfo

	dlog.Debug(ctx, "HandleIntercepts called")

	// Find the chosen intercept if it still exists
	if s.chosenID != "" {
		for _, cept := range cepts {
			if cept.Id == s.chosenID {
				chosenIntercept = cept
				break
			}
		}

		if chosenIntercept != nil {
			// The chosen intercept still exists
			if chosenIntercept.Disposition == manager.InterceptDispositionType_ACTIVE {
				// and is active
				activeIntercept = chosenIntercept
			}
		} else {
			// The chosen intercept was deleted by the user
			dlog.Info(ctx, "The previously-active intercept has been deleted")
			s.chosenID = ""
		}
	} else {
		// Attach to already ACTIVE intercept if there is one.
		for _, cept := range cepts {
			if cept.Disposition == manager.InterceptDispositionType_ACTIVE {
				chosenIntercept = cept
				s.chosenID = cept.Id
				activeIntercept = cept
				break
			}
		}
	}

	// Update forwarding. Find the respective target ports
	for _, fwd := range s.forwarders {
		_, targetPort := fwd.Target()
		if activeIntercept != nil {
			if iceptPort := agent.InterceptPortForContainerPort(activeIntercept.Spec, s.config, targetPort); iceptPort > 0 {
				fwd.SetIntercepting(activeIntercept, iceptPort)
				continue
			}
		}
		fwd.SetIntercepting(nil, 0)
	}

	// Review waiting intercepts
	reviews := []*manager.ReviewInterceptRequest{}
	for _, cept := range cepts {
		if cept.Disposition == manager.InterceptDispositionType_WAITING {
			// This intercept is ready to be active
			switch {
			case chosenIntercept != nil && cept.Id == chosenIntercept.Id:
				// We've already chosen this one and marked it active, but it's not
				// active yet in this snapshot.  We could probably just do nothing
				// and it would probably change to ACTIVE in the very next snapshot
				// because we already marked it active from a previous snapshot and
				// that just hasn't propagated yet.  But let's go ahead and tell the
				// manager to mark it ACTIVE again anyway, just to be safe.
				dlog.Infof(ctx, "Setting intercept %q as ACTIVE (again?)", cept.Id)
				reviews = append(reviews, &manager.ReviewInterceptRequest{
					Id:                cept.Id,
					Disposition:       manager.InterceptDispositionType_ACTIVE,
					PodIp:             s.state.podIP,
					SftpPort:          s.state.sftpPort,
					MountPoint:        s.mountPoint,
					MechanismArgsDesc: "all TCP connections",
					Environment:       s.env,
				})
			case chosenIntercept == nil:
				// We don't have an intercept in play, so choose this one. All
				// agents will get intercepts in the same order every time, so
				// this will yield a consistent result. Note that the intercept
				// will not become active at this time. That will happen later,
				// once the manager assigns a port.
				dlog.Infof(ctx, "Setting intercept %q as ACTIVE", cept.Id)
				s.chosenID = cept.Id
				chosenIntercept = cept
				reviews = append(reviews, &manager.ReviewInterceptRequest{
					Id:                cept.Id,
					Disposition:       manager.InterceptDispositionType_ACTIVE,
					PodIp:             s.state.podIP,
					SftpPort:          s.state.sftpPort,
					MountPoint:        s.mountPoint,
					MechanismArgsDesc: "all TCP connections",
					Environment:       s.env,
				})
			default:
				// We already have an intercept in play, so reject this one.
				dlog.Infof(ctx, "Setting intercept %q as AGENT_ERROR; as it conflicts with %q as the current chosen-to-be-ACTIVE intercept", cept.Id, s.chosenID)
				var msg string
				if chosenIntercept.Disposition == manager.InterceptDispositionType_ACTIVE {
					msg = fmt.Sprintf("Conflicts with the currently-served intercept %q", s.chosenID)
				} else {
					msg = fmt.Sprintf("Conflicts with the currently-waiting-to-be-served intercept %q", s.chosenID)
				}
				reviews = append(reviews, &manager.ReviewInterceptRequest{
					Id:                cept.Id,
					Disposition:       manager.InterceptDispositionType_AGENT_ERROR,
					Message:           msg,
					MechanismArgsDesc: "all TCP connections",
				})
			}
		}
	}

	return reviews
}
