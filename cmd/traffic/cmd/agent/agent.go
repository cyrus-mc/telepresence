package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"gopkg.in/yaml.v3"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/dos"
	"github.com/telepresenceio/telepresence/v2/pkg/forwarder"
	"github.com/telepresenceio/telepresence/v2/pkg/install/agent"
	"github.com/telepresenceio/telepresence/v2/pkg/iputil"
	"github.com/telepresenceio/telepresence/v2/pkg/restapi"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
	"github.com/telepresenceio/telepresence/v2/pkg/version"
)

type Config struct {
	agent.Config
	PodIP string
}

// Keys that aren't useful when running on the local machine
var skipKeys = map[string]bool{
	"HOME":     true,
	"PATH":     true,
	"HOSTNAME": true,
}

func LoadConfig(ctx context.Context) (*Config, error) {
	cf, err := dos.Open(ctx, filepath.Join(agent.ConfigMountPoint, agent.ConfigFile))
	if err != nil {
		return nil, fmt.Errorf("unable to open agent ConfigMap: %w", err)
	}
	defer cf.Close()

	c := Config{}
	if err = yaml.NewDecoder(cf).Decode(&c.Config); err != nil {
		return nil, fmt.Errorf("unable to decode agent ConfigMap: %w", err)
	}
	c.PodIP = dos.Getenv(ctx, "_TEL_AGENT_POD_IP")
	for _, cn := range c.Containers {
		if err := addSecretsMounts(ctx, cn); err != nil {
			return nil, err
		}
	}
	return &c, nil
}

func (c *Config) HasMounts(ctx context.Context) bool {
	for _, cn := range c.Containers {
		if len(cn.Mounts) > 0 {
			return true
		}
	}
	return false
}

// addSecretsMounts adds any token-rotating system secrets directories if they exist
// e.g. /var/run/secrets/kubernetes.io or /var/run/secrets/eks.amazonaws.com
// to the TELEPRESENCE_MOUNTS environment variable
func addSecretsMounts(ctx context.Context, ag *agent.Container) error {
	// This will attempt to handle all the secrets dirs, but will return the first error we encountered.
	secretsDir, err := dos.Open(ctx, "/var/run/secrets")
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return err
	}
	fileInfo, err := secretsDir.ReadDir(-1)
	if err != nil {
		return err
	}
	secretsDir.Close()

	mm := make(map[string]struct{})
	for _, m := range ag.Mounts {
		mm[m] = struct{}{}
	}

	for _, file := range fileInfo {
		// Directories found in /var/run/secrets get a symlink in appmounts
		if !file.IsDir() {
			continue
		}
		dirPath := filepath.Join("/var/run/secrets/", file.Name())
		dlog.Debugf(ctx, "checking agent secrets mount path: %s", dirPath)
		stat, err := dos.Stat(ctx, dirPath)
		if err != nil {
			return err
		}
		if !stat.IsDir() {
			continue
		}
		if _, ok := mm[dirPath]; ok {
			continue
		}

		appMountsPath := filepath.Join(ag.MountPoint, dirPath)
		dlog.Debugf(ctx, "checking appmounts directory: %s", dirPath)
		// Make sure the path doesn't already exist
		_, err = dos.Stat(ctx, appMountsPath)
		if err == nil {
			return fmt.Errorf("appmounts '%s' already exists", appMountsPath)
		}
		dlog.Debugf(ctx, "create appmounts directory: %s", appMountsPath)
		// Add a link to the kubernetes.io directory under {{.AppMounts}}/var/run/secrets
		err = dos.MkdirAll(ctx, filepath.Dir(appMountsPath), 0700)
		if err != nil {
			return err
		}
		dlog.Debugf(ctx, "create appmounts symlink: %s %s", dirPath, appMountsPath)
		err = dos.Symlink(ctx, dirPath, appMountsPath)
		if err != nil {
			return err
		}
		dlog.Infof(ctx, "new agent secrets mount path: %s", dirPath)
		ag.Mounts = append(ag.Mounts, dirPath)
		mm[dirPath] = struct{}{}
	}
	return nil
}

// AppEnvironment returns the environment visible to this agent together with environment variables
// explicitly declared for the app container and minus the environment variables provided by this
// config.
func AppEnvironment(ctx context.Context, ag *agent.Container) (map[string]string, error) {
	osEnv := dos.Environ(ctx)
	prefix := agent.EnvPrefixApp + ag.EnvPrefix
	fullEnv := make(map[string]string, len(osEnv))

	// Add prefixed variables separately last, so that we can
	// ensure that they have higher precedence.
	for _, env := range osEnv {
		if !strings.HasPrefix(env, agent.EnvPrefix) {
			pair := strings.SplitN(env, "=", 2)
			if len(pair) == 2 {
				k := pair[0]
				if _, skip := skipKeys[k]; !skip {
					fullEnv[k] = pair[1]
				}
			}
		}
	}
	for _, env := range osEnv {
		if strings.HasPrefix(env, prefix) {
			pair := strings.SplitN(env, "=", 2)
			if len(pair) == 2 {
				k := pair[0][len(prefix):]
				fullEnv[k] = pair[1]
			}
		}
	}
	fullEnv[agent.EnvInterceptContainer] = ag.Name
	if len(ag.Mounts) > 0 {
		fullEnv[agent.EnvInterceptMounts] = strings.Join(ag.Mounts, ":")
	}
	return fullEnv, nil
}

// SftpServer creates a listener on the next available port, writes that port on the
// given channel, and then starts accepting connections on that port. Each connection
// starts a sftp-server that communicates with that connection using its stdin and stdout.
func SftpServer(ctx context.Context, sftpPortCh chan<- int32) error {
	defer close(sftpPortCh)

	// start an sftp-server for remote sshfs mounts
	lc := net.ListenConfig{}
	l, err := lc.Listen(ctx, "tcp4", ":0")
	if err != nil {
		return err
	}

	// Accept doesn't actually return when the context is cancelled so
	// it's explicitly closed here.
	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()

	_, sftpPort, err := iputil.SplitToIPPort(l.Addr())
	if err != nil {
		return err
	}
	sftpPortCh <- int32(sftpPort)

	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() == nil {
				return fmt.Errorf("listener on sftp-server connection failed: %v", err)
			}
			return nil
		}
		go func() {
			s, err := sftp.NewServer(conn)
			if err != nil {
				dlog.Error(ctx, err)
			}
			dlog.Debugf(ctx, "Serving sftp connection from %s", conn.RemoteAddr())
			if err = s.Serve(); err != nil {
				if !errors.Is(err, io.EOF) {
					dlog.Errorf(ctx, "sftp server completed with error %v", err)
					return
				}
			}
			dlog.Errorf(ctx, "sftp server completed because client exited")
		}()
	}
}

func Main(ctx context.Context, args ...string) error {
	dlog.Infof(ctx, "Traffic Agent %s [pid:%d]", version.Version, os.Getpid())

	// Handle configuration
	config, err := LoadConfig(ctx)
	if err != nil {
		return err
	}

	info := &rpc.AgentInfo{
		Name:      config.AgentName,
		PodIp:     config.PodIP,
		Product:   "telepresence",
		Version:   version.Version,
		Namespace: config.Namespace,
	}

	// Select initial mechanism
	mechanisms := []*rpc.AgentInfo_Mechanism{
		{
			Name:    "tcp",
			Product: "telepresence",
			Version: version.Version,
		},
	}
	info.Mechanisms = mechanisms

	g := dgroup.NewGroup(ctx, dgroup.GroupConfig{
		EnableSignalHandling: true,
	})

	sftpPortCh := make(chan int32)
	if config.HasMounts(ctx) {
		g.Go("sftp-server", func(ctx context.Context) error {
			return SftpServer(ctx, sftpPortCh)
		})
	} else {
		close(sftpPortCh)
		dlog.Info(ctx, "Not starting sftp-server because there's nothing to mount")
	}

	// Talk to the Traffic Manager
	g.Go("client", func(ctx context.Context) error {
		gRPCAddress := fmt.Sprintf("%s:%v", config.ManagerHost, config.ManagerPort)

		// Don't reconnect more than once every five seconds
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		sftpPort := <-sftpPortCh
		state := NewState(config.ManagerHost, config.Namespace, config.PodIP, sftpPort)

		// Manage the forwarders
		for _, cn := range config.Containers {
			env, err := AppEnvironment(ctx, cn)
			if err != nil {
				return err
			}
			fwds := make([]*forwarder.Forwarder, len(cn.Intercepts))
			for i, ic := range cn.Intercepts {
				lisAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", ic.AgentPort))
				if err != nil {
					return err
				}
				fwd := forwarder.NewForwarder(lisAddr, "", ic.ContainerPort)
				fwds[i] = fwd
				g.Go(fmt.Sprintf("forward-%s:%d", cn.Name, ic.ContainerPort), func(ctx context.Context) error {
					return fwd.Serve(tunnel.WithPool(ctx, tunnel.NewPool()))
				})
			}
			state.AddIntercept(fwds, cn, env)
		}

		if config.APIPort != 0 {
			dgroup.ParentGroup(ctx).Go("API-server", func(ctx context.Context) error {
				return restapi.NewServer(state.AgentState()).ListenAndServe(ctx, int(config.APIPort))
			})
		}

		for {
			if err := TalkToManager(ctx, gRPCAddress, info, state); err != nil {
				dlog.Info(ctx, err)
			}

			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	})

	// Wait for exit
	return g.Wait()
}
