package config

import (
	"os"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.DataDir != "./zephyr-data" {
		t.Errorf("expected default dataDir, got %s", config.DataDir)
	}

	if config.ClientPort != 2181 {
		t.Errorf("expected port 2181, got %d", config.ClientPort)
	}

	if config.TickTime != 2000 {
		t.Errorf("expected tickTime 2000, got %d", config.TickTime)
	}

	if config.MaxClientCnxns != 10000 {
		t.Errorf("expected maxClientCnxns 10000, got %d", config.MaxClientCnxns)
	}
}

func TestLoadConfig(t *testing.T) {
	// Create temp config file
	content := `# Test config
dataDir=/tmp/zephyr-test
clientPort=2182
tickTime=3000
maxClientCnxns=5000
initLimit=5
syncLimit=2
minSessionTimeout=2000
maxSessionTimeout=60000
`
	tmpFile, err := os.CreateTemp("", "zephyr-*.cfg")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}
	tmpFile.Close()

	config, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if config.DataDir != "/tmp/zephyr-test" {
		t.Errorf("expected /tmp/zephyr-test, got %s", config.DataDir)
	}

	if config.ClientPort != 2182 {
		t.Errorf("expected port 2182, got %d", config.ClientPort)
	}

	if config.TickTime != 3000 {
		t.Errorf("expected tickTime 3000, got %d", config.TickTime)
	}

	if config.MaxClientCnxns != 5000 {
		t.Errorf("expected maxClientCnxns 5000, got %d", config.MaxClientCnxns)
	}

	if config.InitLimit != 5 {
		t.Errorf("expected initLimit 5, got %d", config.InitLimit)
	}

	if config.MinSessionTimeout != 2000 {
		t.Errorf("expected minSessionTimeout 2000, got %d", config.MinSessionTimeout)
	}
}

func TestLoadConfig_ServerDef(t *testing.T) {
	content := `dataDir=/tmp/test
server.1=host1:2888:3888
server.2=host2:2888:3888
server.3=host3:2888:3888
`
	tmpFile, err := os.CreateTemp("", "zephyr-cluster-*.cfg")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}
	tmpFile.Close()

	config, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if len(config.Servers) != 3 {
		t.Errorf("expected 3 servers, got %d", len(config.Servers))
	}

	server1, ok := config.Servers[1]
	if !ok {
		t.Error("server 1 not found")
	} else {
		if server1.Host != "host1" {
			t.Errorf("expected host1, got %s", server1.Host)
		}
		if server1.QuorumPort != 2888 {
			t.Errorf("expected quorum port 2888, got %d", server1.QuorumPort)
		}
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "valid default",
			config:  DefaultConfig(),
			wantErr: false,
		},
		{
			name:    "empty dataDir",
			config:  &Config{DataDir: "", ClientPort: 2181, TickTime: 2000},
			wantErr: true,
		},
		{
			name:    "invalid port",
			config:  &Config{DataDir: "/tmp", ClientPort: 0, TickTime: 2000},
			wantErr: true,
		},
		{
			name:    "invalid tickTime",
			config:  &Config{DataDir: "/tmp", ClientPort: 2181, TickTime: 0},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.Validate()
			if tc.wantErr && err == nil {
				t.Error("expected error but got none")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestConfig_ToServerConfig(t *testing.T) {
	config := DefaultConfig()
	config.ClientPort = 2183
	config.MaxClientCnxns = 500
	config.DataDir = "/var/zephyr"

	serverConfig := config.ToServerConfig()

	if serverConfig.DataDir != "/var/zephyr" {
		t.Errorf("expected /var/zephyr, got %s", serverConfig.DataDir)
	}

	if serverConfig.Transport.ListenAddr != ":2183" {
		t.Errorf("expected :2183, got %s", serverConfig.Transport.ListenAddr)
	}

	if serverConfig.Transport.MaxConnections != 500 {
		t.Errorf("expected 500, got %d", serverConfig.Transport.MaxConnections)
	}
}

func TestWriteExample(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "zephyr-example-*.cfg")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	err = WriteExample(tmpFile.Name())
	if err != nil {
		t.Fatalf("WriteExample failed: %v", err)
	}

	// Should be loadable
	config, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to load example config: %v", err)
	}

	if config.ClientPort != 2181 {
		t.Errorf("expected port 2181 in example, got %d", config.ClientPort)
	}
}
