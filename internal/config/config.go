// Package config provides configuration parsing for ZephyrCoord.
package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ichbingautam/zephyr-coord/internal/server"
)

// Config represents the complete server configuration.
type Config struct {
	// Basic settings
	DataDir    string `json:"dataDir"`
	ClientPort int    `json:"clientPort"`

	// Timeouts
	TickTime          int `json:"tickTime"`          // milliseconds
	InitLimit         int `json:"initLimit"`         // ticks
	SyncLimit         int `json:"syncLimit"`         // ticks
	MinSessionTimeout int `json:"minSessionTimeout"` // milliseconds
	MaxSessionTimeout int `json:"maxSessionTimeout"` // milliseconds

	// Limits
	MaxClientCnxns int `json:"maxClientCnxns"`
	MaxRequestSize int `json:"maxRequestSize"`

	// Cluster
	ServerID int               `json:"serverId"`
	Servers  map[int]ServerDef `json:"servers"`

	// Advanced
	AutoPurge       bool `json:"autopurge.snapRetainCount"`
	SnapRetainCount int  `json:"snapRetainCount"`
	PurgeInterval   int  `json:"purgeInterval"` // hours
	AdminEnable     bool `json:"admin.enableServer"`
	AdminPort       int  `json:"admin.serverPort"`
	MetricsEnable   bool `json:"metricsProvider.enabled"`
}

// ServerDef defines a server in the ensemble.
type ServerDef struct {
	Host       string
	ClientPort int
	QuorumPort int
	ElectPort  int
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		DataDir:           "./zephyr-data",
		ClientPort:        2181,
		TickTime:          2000,
		InitLimit:         10,
		SyncLimit:         5,
		MinSessionTimeout: 4000,
		MaxSessionTimeout: 40000,
		MaxClientCnxns:    10000,
		MaxRequestSize:    10 * 1024 * 1024, // 10MB
		SnapRetainCount:   3,
		PurgeInterval:     0, // disabled
		AdminEnable:       true,
		AdminPort:         8080,
		MetricsEnable:     true,
		Servers:           make(map[int]ServerDef),
	}
}

// LoadConfig loads configuration from a file.
func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config: %w", err)
	}
	defer file.Close()

	config := DefaultConfig()

	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip comments and empty lines
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse key=value
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("line %d: invalid format", lineNum)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		if err := config.set(key, value); err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNum, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading config: %w", err)
	}

	return config, nil
}

// set applies a key-value configuration.
func (c *Config) set(key, value string) error {
	switch key {
	case "dataDir":
		c.DataDir = value
	case "clientPort":
		port, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.ClientPort = port
	case "tickTime":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.TickTime = v
	case "initLimit":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.InitLimit = v
	case "syncLimit":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.SyncLimit = v
	case "minSessionTimeout":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.MinSessionTimeout = v
	case "maxSessionTimeout":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.MaxSessionTimeout = v
	case "maxClientCnxns":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.MaxClientCnxns = v
	case "autopurge.snapRetainCount":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.SnapRetainCount = v
	case "autopurge.purgeInterval":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.PurgeInterval = v
	case "admin.enableServer":
		c.AdminEnable = value == "true"
	case "admin.serverPort":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.AdminPort = v
	case "metricsProvider.enabled":
		c.MetricsEnable = value == "true"
	default:
		// Check for server definition: server.N=host:port:port:port
		if strings.HasPrefix(key, "server.") {
			idStr := strings.TrimPrefix(key, "server.")
			id, err := strconv.Atoi(idStr)
			if err != nil {
				return fmt.Errorf("invalid server id: %s", idStr)
			}

			parts := strings.Split(value, ":")
			if len(parts) < 3 {
				return fmt.Errorf("invalid server definition: %s", value)
			}

			def := ServerDef{Host: parts[0]}
			def.QuorumPort, _ = strconv.Atoi(parts[1])
			def.ElectPort, _ = strconv.Atoi(parts[2])
			if len(parts) > 3 {
				def.ClientPort, _ = strconv.Atoi(parts[3])
			}

			c.Servers[id] = def
		}
		// Ignore unknown keys for forward compatibility
	}
	return nil
}

// ToServerConfig converts to internal server config.
func (c *Config) ToServerConfig() server.ServerConfig {
	config := server.DefaultServerConfig(c.DataDir)
	config.Transport.ListenAddr = fmt.Sprintf(":%d", c.ClientPort)
	config.Transport.MaxConnections = c.MaxClientCnxns
	config.Session.MinSessionTimeout = time.Duration(c.MinSessionTimeout) * time.Millisecond
	config.Session.MaxSessionTimeout = time.Duration(c.MaxSessionTimeout) * time.Millisecond
	return config
}

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("dataDir is required")
	}
	if c.ClientPort <= 0 || c.ClientPort > 65535 {
		return fmt.Errorf("invalid clientPort: %d", c.ClientPort)
	}
	if c.TickTime < 1 {
		return fmt.Errorf("tickTime must be positive")
	}
	if c.MaxClientCnxns < 0 {
		return fmt.Errorf("maxClientCnxns cannot be negative")
	}
	return nil
}

// WriteExample writes an example configuration file.
func WriteExample(path string) error {
	example := `# ZephyrCoord Configuration
# See https://zookeeper.apache.org/doc/current/zookeeperAdmin.html

# The number of milliseconds of each tick
tickTime=2000

# The number of ticks that the initial synchronization phase can take
initLimit=10

# The number of ticks that can pass between sending a request and getting an acknowledgement
syncLimit=5

# The directory where the snapshot is stored
dataDir=./zephyr-data

# The port at which the clients will connect
clientPort=2181

# Maximum number of client connections
maxClientCnxns=10000

# Session timeouts
minSessionTimeout=4000
maxSessionTimeout=40000

# Autopurge (snapshots)
#autopurge.snapRetainCount=3
#autopurge.purgeInterval=1

# Admin server
admin.enableServer=true
admin.serverPort=8080

# Metrics
metricsProvider.enabled=true

# Cluster configuration (for ensemble)
# server.1=host1:2888:3888
# server.2=host2:2888:3888
# server.3=host3:2888:3888
`
	return os.WriteFile(path, []byte(example), 0644)
}
