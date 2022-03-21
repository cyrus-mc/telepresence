package agent

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/v2/pkg/dos"
	"github.com/telepresenceio/telepresence/v2/pkg/dos/aferofs"
	"github.com/telepresenceio/telepresence/v2/pkg/install/agent"
)

const namespace = "teltest"
const podIP = "192.168.50.34"

var testConfig = agent.Config{
	Create:       false,
	AgentImage:   "docker.io/datawire/tel2:2.5.4",
	AgentName:    "test-echo",
	LogLevel:     "debug",
	Namespace:    namespace,
	WorkloadName: "test-echo",
	WorkloadKind: "Deployment",
	ManagerHost:  "traffic-manager.ambassador",
	ManagerPort:  8081,
	APIPort:      0,
	Containers: []*agent.Container{{
		Name:       "test-echo",
		EnvPrefix:  "A_",
		MountPoint: "/tel_app_mounts/test-echo",
		Mounts:     []string{"/home/bob"},
		Intercepts: []*agent.Intercept{
			{
				ContainerPortName: "http",
				ServiceName:       "test-echo",
				ServiceUID:        "",
				ServicePortName:   "http",
				Protocol:          "TCP",
				AgentPort:         9900,
				ContainerPort:     8080,
			},
		},
	}},
}

func testContext(t *testing.T, env dos.MapEnv) context.Context {
	fs := afero.NewBasePathFs(afero.NewOsFs(), t.TempDir())
	if env == nil {
		env = make(dos.MapEnv)
	}
	y, err := yaml.Marshal(&testConfig)
	require.NoError(t, err)

	require.NoError(t, fs.MkdirAll(agent.ConfigMountPoint, 0700))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(agent.ConfigMountPoint, agent.ConfigFile), y, 0600))

	env[agent.EnvPrefixAgent+"POD_IP"] = podIP

	ctx := dlog.NewTestContext(t, false)
	ctx = dos.WithFS(ctx, aferofs.Wrap(fs))
	return dos.WithEnv(ctx, env)
}

func Test_LoadConfig(t *testing.T) {
	ctx := testContext(t, nil)
	config, err := LoadConfig(ctx)
	require.NoError(t, err)
	require.Equal(t, namespace, config.Namespace)
	require.Equal(t, podIP, config.PodIP)
	require.Equal(t, &testConfig, &config.Config)
}

func Test_AppEnvironment(t *testing.T) {
	ctx := testContext(t, dos.MapEnv{
		"HOME":                              "/home/tel",                    // skip
		"PATH":                              "/bin:/usr/bin:/usr/local/bin", // skip
		"ZULU":                              "zulu",                         // include,
		agent.EnvPrefixApp + "A_" + "ALPHA": "alpha",                        // include
		agent.EnvPrefixApp + "B_" + "BRAVO": "bravo",                        // skip
	})

	ksDir := "/var/run/secrets/kubernetes.io/serviceaccount"
	require.NoError(t, dos.MkdirAll(ctx, ksDir, 0700))
	f, err := dos.Create(ctx, filepath.Join(ksDir, "namespace"))
	require.NoError(t, err)
	_, err = fmt.Fprintln(f, "default")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	config, err := LoadConfig(ctx)
	require.NoError(t, err)

	cn := config.Containers[0]
	env, err := AppEnvironment(ctx, cn)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"ALPHA":                  "alpha",
		"ZULU":                   "zulu",
		agent.EnvInterceptMounts: "/home/bob:/var/run/secrets/kubernetes.io",
	}, env)

	// Check symlink to container's remote mount point
	f, err = dos.Open(ctx, filepath.Join(cn.MountPoint, ksDir, "namespace"))
	require.NoError(t, err, "not symlinked")
	data, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Equal(t, "default\n", string(data))
}
