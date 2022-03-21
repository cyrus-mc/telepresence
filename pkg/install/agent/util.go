package agent

import (
	"strconv"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
)

// SpecMatchesIntercept answers the question if an InterceptSpec matches the given
// Intercept config. The spec matches if:
//   - its ServiceName is unset or the ServiceName equals the config's ServiceName
//   - one of its ServicePortIdentifiers:
//      - can be parsed to an integer equal to the config's ServicePort
//      - is equal to the config's ServiceName
//
// A match caused by an undefined identifier has lower priority than a true match
// The function returns the index into the ServicePortIdentifiers/TargetPorts slice
// or -1 if no match was found
func SpecMatchesIntercept(spec *manager.InterceptSpec, ic *Intercept) int {
	if spec.ServiceName != "" && ic.ServiceName != spec.ServiceName {
		return -1
	}
	for i, spi := range spec.ServicePortIdentifiers {
		if IsInterceptFor(spi, ic) {
			return i
		}
	}
	return -1
}

// IsInterceptFor returns true when the given ServicePortIdentifier matches the ServicePortName
// or ServicePort in the given Intercept
func IsInterceptFor(spi string, ic *Intercept) bool {
	if pn, err := strconv.Atoi(spi); err == nil {
		if ic.ServicePort == uint16(pn) {
			return true
		}
	} else if spi == ic.ServicePortName {
		return true
	}
	return false
}

// InterceptPortForContainerPort returns the intercept's target port for the given container port, or 0 if
// no such port could be found. The port is found using the spec's ServicePortIdentifiers, and it is assumed
// that they all have a valid (name or number of a service port) when this function is called
func InterceptPortForContainerPort(spec *manager.InterceptSpec, cn *Container, containerPort uint16) uint16 {
	for _, ic := range cn.Intercepts {
		if ic.ContainerPort == containerPort {
			for i, spi := range spec.ServicePortIdentifiers {
				if IsInterceptFor(spi, ic) {
					return uint16(spec.TargetPorts[i])
				}
			}
		}
	}
	return 0
}
