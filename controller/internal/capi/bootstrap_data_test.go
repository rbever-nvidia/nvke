// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

const testControllerClientCA = "-----BEGIN CERTIFICATE-----\npublic-ca\n-----END CERTIFICATE-----\n"

type testCloudInitConfig struct {
	WriteFiles []testCloudInitFile `yaml:"write_files"`
	RunCmd     []string            `yaml:"runcmd"`
}

type testCloudInitFile struct {
	Path        string `yaml:"path"`
	Owner       string `yaml:"owner"`
	Permissions string `yaml:"permissions"`
	Encoding    string `yaml:"encoding"`
	Content     string `yaml:"content"`
}

func TestRenderBootstrapData(t *testing.T) {
	t.Parallel()

	baseConfig := config.Config{
		DgxcTenantUser:     "test-admin",
		DgxcTenantPassword: "test-password",
		NodeAgent:          config.NodeAgentConfig{Port: "15005"},
		SpireServerAddress: "spire.example.com",
		SpireServerPort:    "9091",
	}
	tlsJWTConfig := baseConfig
	tlsJWTConfig.NodeAgent.TLSJWT.Enabled = true
	tlsJWTConfig.NodeAgent.TLSJWT.DSXIdentityExchangeURL = "https://identity.example.test/v1/exchange"
	externalMTLSConfig := baseConfig
	externalMTLSConfig.NodeAgentExternalMTLSEnabled = true
	combinedConfig := externalMTLSConfig
	combinedConfig.NodeAgent.TLSJWT.Enabled = true
	combinedConfig.NodeAgent.TLSJWT.DSXIdentityExchangeURL = "https://identity.example.test/v1/exchange"

	const profileFieldsTemplate = `
username={{ .Username }}
password={{ .HashedPassword }}
hostname={{ .Hostname }}
kubernetesVersion={{ trimPrefix .KubernetesVersion "v" }}
nodeAgentPort={{ .NodeAgentPort }}
diskSelectionStrategy={{ .DiskSelectionStrategy }}
calicoEnabled={{ .CalicoEnabled }}
ovnEnabled={{ .OvnEnabled }}
cniImagePackage={{ .CNIImagePackage }}
cniChartsPackage={{ .CNIChartsPackage }}
files={{ len .Files }}
spiffeEnabled={{ .SpiffeEnabled }}
spireServer={{ .SpireServerAddress }}:{{ .SpireServerPort }}
trustDomain={{ .TrustDomain }}
trustBundle:
{{ indent 2 .TrustBundle }}
teleportEnabled={{ .TeleportEnabled }}
teleportToken={{ .TeleportTokenName }}
teleportProxy={{ .TeleportProxyAddress }}
teleportJoinMethod={{ .TeleportJoinMethod }}
teleportLabels={{ .TeleportLabels }}
`
	const cloudConfigTemplate = `#cloud-config
autoinstall:
  version: 1
  identity:
    hostname: {{ .Hostname }}
  user-data:
    write_files:
{{- range .Files }}
      - path: {{ quote .Path }}
        owner: {{ quote .Owner }}
        permissions: {{ quote .Permissions }}
{{- if .Encoding }}
        encoding: {{ quote .Encoding }}
{{- end }}
        content: {{ quote .Content }}
{{- end }}
    runcmd:
      - systemctl restart nke-agent
`

	tests := map[string]struct {
		cfg                  config.Config
		template             string
		input                BootstrapDataInput
		emptyControllerCA    bool
		expectBundleLookup   bool
		wantErr              string
		verify               func(*testing.T, string)
	}{
		"renders profile fields": {
			cfg:      baseConfig,
			template: profileFieldsTemplate,
			input: BootstrapDataInput{
				Hostname:              "test-node",
				SpiffeEnabled:         true,
				DiskSelectionStrategy: db.DiskSelectionStrategyLargest,
				KubernetesVersion:     "v1.33.0",
				Cluster:               db.Cluster{CniPlugin: db.CNICalico, CniVersion: "3.31.5"},
				Teleport: TeleportBootstrapConfig{
					Enabled:      true,
					TokenName:    "test-token",
					ProxyAddress: "teleport.example.com:443",
					JoinMethod:   "tpm",
					Labels:       "environment: test",
				},
				IncludeSpireTrustBundle: true,
			},
			expectBundleLookup: true,
			verify: func(t *testing.T, userData string) {
				t.Helper()
				assert.Contains(t, userData, "username=test-admin\n")
				assert.Regexp(t, `(?m)^password=\$6\$[^\n]+$`, userData)
				assert.NotContains(t, userData, "password=test-password")
				assert.Contains(t, userData, "hostname=test-node\n")
				assert.Contains(t, userData, "kubernetesVersion=1.33.0\n")
				assert.Contains(t, userData, "nodeAgentPort=15005\n")
				assert.Contains(t, userData, "diskSelectionStrategy=largest\n")
				assert.Contains(t, userData, "calicoEnabled=true\n")
				assert.Contains(t, userData, "ovnEnabled=false\n")
				assert.Contains(t, userData, "cniImagePackage=nke-cni-calico-images-3.31.5\n")
				assert.Contains(t, userData, "cniChartsPackage=nke-cni-calico-manifests-3.31.5\n")
				assert.Contains(t, userData, "files=1\n")
				assert.Contains(t, userData, "spiffeEnabled=true\n")
				assert.Contains(t, userData, "spireServer=spire.example.com:9091\n")
				assert.Contains(t, userData, "trustDomain=example.com\n")
				assert.Contains(t, userData, "trustBundle:\n  first certificate\n  second certificate\n")
				assert.Contains(t, userData, "teleportEnabled=true\n")
				assert.Contains(t, userData, "teleportToken=test-token\n")
				assert.Contains(t, userData, "teleportProxy=teleport.example.com:443\n")
				assert.Contains(t, userData, "teleportJoinMethod=tpm\n")
				assert.Contains(t, userData, "teleportLabels=environment: test\n")
			},
		},
		"renders OVN package names": {
			cfg:      baseConfig,
			template: "images={{ .CNIImagePackage }}\ncharts={{ .CNIChartsPackage }}\n",
			input: BootstrapDataInput{
				Cluster: db.Cluster{CniPlugin: db.CNIOvnKubernetes, CniVersion: "1.4.1"},
			},
			verify: func(t *testing.T, userData string) {
				t.Helper()
				assert.Equal(t, "images=nke-cni-ovn-images-1.4.1\ncharts=nke-cni-ovn-charts-1.4.1\n", userData)
			},
		},
		"combines TLS JWT and external mTLS bootstrap": {
			cfg:      combinedConfig,
			template: cloudConfigTemplate,
			input: BootstrapDataInput{
				Hostname: "test-node",
				Cluster:  db.Cluster{CniPlugin: db.CNICalico, CniVersion: "3.31.5"},
			},
			verify: func(t *testing.T, userData string) {
				t.Helper()
				assert.Contains(t, userData, "--grpc-auth-mode=tls-jwt")
				assert.Contains(t, userData, "--dsx-identity-exchange-url=https://identity.example.test/v1/exchange")
				assert.Contains(t, userData, config.NodeAgentControllerCAPath)
				assert.Contains(t, userData, base64.StdEncoding.EncodeToString([]byte(testControllerClientCA)))
				assert.Contains(t, userData, "--controller-client-ca-bundle="+config.NodeAgentControllerCAPath)

				var rendered struct {
					Autoinstall struct {
						UserData testCloudInitConfig `yaml:"user-data"`
					} `yaml:"autoinstall"`
				}
				require.NoError(t, yaml.Unmarshal([]byte(userData), &rendered))
				require.Len(t, rendered.Autoinstall.UserData.WriteFiles, 2)
				assert.Equal(t, []string{"systemctl restart nke-agent"}, rendered.Autoinstall.UserData.RunCmd)
			},
		},
		"external mTLS requires a controller CA": {
			cfg:               externalMTLSConfig,
			template:          "files={{ len .Files }}\n",
			input:             BootstrapDataInput{Cluster: db.Cluster{CniPlugin: db.CNICalico, CniVersion: "3.31.5"}},
			emptyControllerCA: true,
			wantErr:           "controller client CA bundle is required when external node-agent mTLS is enabled",
		},
	}

	for name, params := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			bundleLookupCalled := false
			controllerCA := testControllerClientCA
			if params.emptyControllerCA {
				controllerCA = ""
			}
			params.input.Config = params.cfg
			params.input.ControllerClientCABundle = controllerCA
			params.input.GetTrustDomainBundle = func(context.Context, config.Config) (string, string, error) {
				bundleLookupCalled = true
				return "example.com", "first certificate\nsecond certificate\n", nil
			}

			userData, err := RenderBootstrapData(context.Background(), params.template, params.input)
			if params.wantErr != "" {
				require.ErrorContains(t, err, params.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, params.expectBundleLookup, bundleLookupCalled)
			params.verify(t, userData)
		})
	}
}

func TestRenderBootstrapDataProfileFixtures(t *testing.T) {
	t.Parallel()

	fixturePaths := []string{
		filepath.Join("..", "..", "profiles", "example", "profiles", "osProfiles", "example-ubuntu-24.04-amd64", "userData.tmpl"),
		filepath.Join("..", "..", "test", "fixtures", "profiles", "osProfiles", "nke-local-dev-kindest-arm64", "userData.tmpl"),
		filepath.Join("..", "..", "test", "fixtures", "profiles", "osProfiles", "nke-nico-vms-24.04-amd64-fde", "userData.tmpl"),
		filepath.Join("..", "..", "test", "fixtures", "profiles", "osProfiles", "nke-nico-vms-24.04-arm64-fde", "userData.tmpl"),
		filepath.Join("..", "..", "test", "fixtures", "profiles", "osProfiles", "test-fixture-amd64", "userData.tmpl"),
		filepath.Join("..", "..", "nico-sim", "test", "integration", "testutil", "testdata", "prod-userdata.tmpl"),
	}

	for _, fixturePath := range fixturePaths {
		t.Run(fixturePath, func(t *testing.T) {
			t.Parallel()
			templateBytes, err := os.ReadFile(fixturePath)
			require.NoError(t, err)
			input := BootstrapDataInput{
				Config: config.Config{
					DgxcTenantUser:               "test-admin",
					DgxcTenantPassword:           "test-password",
					NodeAgentExternalMTLSEnabled: true,
					NodeAgent: config.NodeAgentConfig{
						Port:                   "15005",
						SpireAttestationMethod: config.SpireAttestationMethodTPM,
					},
				},
				ControllerClientCABundle: testControllerClientCA,
				GetTrustDomainBundle: func(context.Context, config.Config) (string, string, error) {
					return "example.com", "first certificate\nsecond certificate\n", nil
				},
				Hostname:                 "test-node",
				SpiffeEnabled:            true,
				DiskSelectionStrategy:    db.DiskSelectionStrategyLargest,
				KubernetesVersion:        "1.33.0",
				Cluster:                  db.Cluster{CniPlugin: db.CNICalico, CniVersion: "3.31.5"},
				IncludeSpireTrustBundle:  true,
			}

			userData, err := RenderBootstrapData(context.Background(), string(templateBytes), input)
			require.NoError(t, err)
			assert.Equal(t, 1, strings.Count(userData, "write_files:"))
			assert.Equal(t, 1, strings.Count(userData, "path: \""+config.NodeAgentConfigPath+"\""))
			assert.NotContains(t, userData, "printf 'AGENT_ARGS=")
			assert.Contains(t, userData, "--controller-client-ca-bundle="+config.NodeAgentControllerCAPath)

			writeFiles := parseBootstrapFiles(t, userData)
			controllerCAFile := findBootstrapFile(writeFiles, config.NodeAgentControllerCAPath)
			agentConfigFile := findBootstrapFile(writeFiles, config.NodeAgentConfigPath)
			require.NotNil(t, controllerCAFile)
			require.NotNil(t, agentConfigFile)
			decodedCA, err := base64.StdEncoding.DecodeString(controllerCAFile.Content)
			require.NoError(t, err)
			assert.Equal(t, testControllerClientCA, string(decodedCA))
			assert.Contains(t, agentConfigFile.Content, "--secure-server-port 15005")
			assert.Contains(t, agentConfigFile.Content, "--controller-client-ca-bundle="+config.NodeAgentControllerCAPath)

			input.Config.NodeAgentExternalMTLSEnabled = false
			input.ControllerClientCABundle = ""
			userData, err = RenderBootstrapData(context.Background(), string(templateBytes), input)
			require.NoError(t, err)
			assert.NotContains(t, userData, "--controller-client-ca-bundle")
			writeFiles = parseBootstrapFiles(t, userData)
			assert.Nil(t, findBootstrapFile(writeFiles, config.NodeAgentControllerCAPath))
			agentConfigFile = findBootstrapFile(writeFiles, config.NodeAgentConfigPath)
			require.NotNil(t, agentConfigFile)
			assert.Equal(t, `AGENT_ARGS="--secure-server-port 15005"`+"\n", agentConfigFile.Content)
		})
	}
}

func parseBootstrapFiles(t *testing.T, userData string) []testCloudInitFile {
	t.Helper()
	var cloudConfig struct {
		WriteFiles  []testCloudInitFile `yaml:"write_files"`
		Autoinstall struct {
			UserData struct {
				WriteFiles []testCloudInitFile `yaml:"write_files"`
			} `yaml:"user-data"`
		} `yaml:"autoinstall"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(userData), &cloudConfig))
	if len(cloudConfig.WriteFiles) > 0 {
		return cloudConfig.WriteFiles
	}
	return cloudConfig.Autoinstall.UserData.WriteFiles
}

func findBootstrapFile(files []testCloudInitFile, path string) *testCloudInitFile {
	for index := range files {
		if files[index].Path == path {
			return &files[index]
		}
	}
	return nil
}
