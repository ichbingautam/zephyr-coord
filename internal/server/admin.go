// Package server - Admin command handlers.
package server

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
)

// AdminCommand represents an admin command type.
type AdminCommand string

const (
	CmdStat AdminCommand = "stat"
	CmdSrvr AdminCommand = "srvr"
	CmdConf AdminCommand = "conf"
	CmdCons AdminCommand = "cons"
	CmdDump AdminCommand = "dump"
	CmdEnvi AdminCommand = "envi"
	CmdRuok AdminCommand = "ruok"
	CmdSrst AdminCommand = "srst"
	CmdWchs AdminCommand = "wchs"
	CmdWchc AdminCommand = "wchc"
	CmdWchp AdminCommand = "wchp"
	CmdMntr AdminCommand = "mntr"
	CmdIsro AdminCommand = "isro"
)

// AdminHandler handles admin commands.
type AdminHandler struct {
	server    *Server
	startTime time.Time

	// Counters
	requestsReceived atomic.Int64
	packetsReceived  atomic.Int64
	packetsSent      atomic.Int64
}

// NewAdminHandler creates a new admin handler.
func NewAdminHandler(server *Server) *AdminHandler {
	return &AdminHandler{
		server:    server,
		startTime: time.Now(),
	}
}

// Handle processes an admin command.
func (h *AdminHandler) Handle(cmd AdminCommand) ([]byte, error) {
	switch cmd {
	case CmdRuok:
		return []byte("imok"), nil
	case CmdStat:
		return h.handleStat()
	case CmdSrvr:
		return h.handleSrvr()
	case CmdConf:
		return h.handleConf()
	case CmdEnvi:
		return h.handleEnvi()
	case CmdMntr:
		return h.handleMntr()
	case CmdIsro:
		return h.handleIsro()
	case CmdWchs:
		return h.handleWchs()
	case CmdCons:
		return h.handleCons()
	case CmdSrst:
		return h.handleSrst()
	default:
		return nil, fmt.Errorf("unknown command: %s", cmd)
	}
}

// handleStat returns server statistics.
func (h *AdminHandler) handleStat() ([]byte, error) {
	stats := h.server.Stats()

	var output string
	output += fmt.Sprintf("ZephyrCoord version: 1.0.0\n")
	output += fmt.Sprintf("Clients: %d\n", stats.Transport.ActiveConnections)
	output += fmt.Sprintf("Received: %d\n", h.packetsReceived.Load())
	output += fmt.Sprintf("Sent: %d\n", h.packetsSent.Load())
	output += fmt.Sprintf("Connections: %d\n", stats.Transport.TotalConnections)
	output += fmt.Sprintf("Outstanding: 0\n")
	output += fmt.Sprintf("Zxid: 0x%x\n", stats.Datastore.ZXID)
	output += fmt.Sprintf("Mode: standalone\n")
	output += fmt.Sprintf("Node count: %d\n", stats.Datastore.NodeCount)

	return []byte(output), nil
}

// handleSrvr returns server configuration.
func (h *AdminHandler) handleSrvr() ([]byte, error) {
	stats := h.server.Stats()
	uptime := time.Since(h.startTime)

	var output string
	output += fmt.Sprintf("ZephyrCoord version: 1.0.0\n")
	output += fmt.Sprintf("Latency min/avg/max: 0/0/0\n")
	output += fmt.Sprintf("Received: %d\n", h.packetsReceived.Load())
	output += fmt.Sprintf("Sent: %d\n", h.packetsSent.Load())
	output += fmt.Sprintf("Connections: %d\n", stats.Transport.ActiveConnections)
	output += fmt.Sprintf("Outstanding: 0\n")
	output += fmt.Sprintf("Zxid: 0x%x\n", stats.Datastore.ZXID)
	output += fmt.Sprintf("Mode: standalone\n")
	output += fmt.Sprintf("Node count: %d\n", stats.Datastore.NodeCount)
	output += fmt.Sprintf("Uptime: %s\n", uptime.Round(time.Second))

	return []byte(output), nil
}

// handleConf returns configuration info.
func (h *AdminHandler) handleConf() ([]byte, error) {
	config := h.server.config

	var output string
	output += fmt.Sprintf("clientPort=%s\n", config.Transport.ListenAddr)
	output += fmt.Sprintf("dataDir=%s\n", config.DataDir)
	output += fmt.Sprintf("tickTime=2000\n")
	output += fmt.Sprintf("maxClientCnxns=%d\n", config.Transport.MaxConnections)
	output += fmt.Sprintf("minSessionTimeout=4000\n")
	output += fmt.Sprintf("maxSessionTimeout=40000\n")
	output += fmt.Sprintf("serverId=0\n")

	return []byte(output), nil
}

// handleEnvi returns environment info.
func (h *AdminHandler) handleEnvi() ([]byte, error) {
	var output string
	output += fmt.Sprintf("Environment:\n")
	output += fmt.Sprintf("zephyr.version=1.0.0\n")
	output += fmt.Sprintf("host.name=%s\n", "localhost")
	output += fmt.Sprintf("java.version=go%s\n", runtime.Version())
	output += fmt.Sprintf("os.name=%s\n", runtime.GOOS)
	output += fmt.Sprintf("os.arch=%s\n", runtime.GOARCH)
	output += fmt.Sprintf("os.version=\n")
	output += fmt.Sprintf("user.dir=%s\n", h.server.config.DataDir)

	return []byte(output), nil
}

// handleMntr returns detailed monitoring metrics.
func (h *AdminHandler) handleMntr() ([]byte, error) {
	stats := h.server.Stats()
	uptime := time.Since(h.startTime)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	watchCount := stats.Watches.DataWatchCount + stats.Watches.ChildWatchCount

	var output string
	output += fmt.Sprintf("zk_version\t1.0.0\n")
	output += fmt.Sprintf("zk_avg_latency\t0\n")
	output += fmt.Sprintf("zk_max_latency\t0\n")
	output += fmt.Sprintf("zk_min_latency\t0\n")
	output += fmt.Sprintf("zk_packets_received\t%d\n", h.packetsReceived.Load())
	output += fmt.Sprintf("zk_packets_sent\t%d\n", h.packetsSent.Load())
	output += fmt.Sprintf("zk_num_alive_connections\t%d\n", stats.Transport.ActiveConnections)
	output += fmt.Sprintf("zk_outstanding_requests\t0\n")
	output += fmt.Sprintf("zk_server_state\tstandalone\n")
	output += fmt.Sprintf("zk_znode_count\t%d\n", stats.Datastore.NodeCount)
	output += fmt.Sprintf("zk_watch_count\t%d\n", watchCount)
	output += fmt.Sprintf("zk_ephemerals_count\t0\n")
	output += fmt.Sprintf("zk_approximate_data_size\t0\n")
	output += fmt.Sprintf("zk_open_file_descriptor_count\t0\n")
	output += fmt.Sprintf("zk_max_file_descriptor_count\t0\n")
	output += fmt.Sprintf("zk_uptime\t%d\n", int64(uptime.Seconds()))
	output += fmt.Sprintf("zk_heap_alloc\t%d\n", m.HeapAlloc)
	output += fmt.Sprintf("zk_heap_sys\t%d\n", m.HeapSys)
	output += fmt.Sprintf("zk_num_goroutines\t%d\n", runtime.NumGoroutine())

	return []byte(output), nil
}

// handleIsro returns read-only status.
func (h *AdminHandler) handleIsro() ([]byte, error) {
	// For now, always return "rw" (read-write mode)
	return []byte("rw"), nil
}

// handleWchs returns watch summary.
func (h *AdminHandler) handleWchs() ([]byte, error) {
	stats := h.server.Stats()
	watchCount := stats.Watches.DataWatchCount + stats.Watches.ChildWatchCount

	var output string
	output += fmt.Sprintf("%d connections watching %d paths\n",
		stats.Transport.ActiveConnections, watchCount)
	output += fmt.Sprintf("Total watches:%d\n", watchCount)

	return []byte(output), nil
}

// handleCons returns connection summary.
func (h *AdminHandler) handleCons() ([]byte, error) {
	stats := h.server.Stats()
	return []byte(fmt.Sprintf("Active connections: %d\n", stats.Transport.ActiveConnections)), nil
}

// handleSrst resets statistics.
func (h *AdminHandler) handleSrst() ([]byte, error) {
	h.packetsReceived.Store(0)
	h.packetsSent.Store(0)
	h.requestsReceived.Store(0)
	return []byte("Server stats reset.\n"), nil
}

// RecordPacketReceived increments received packet counter.
func (h *AdminHandler) RecordPacketReceived() {
	h.packetsReceived.Add(1)
}

// RecordPacketSent increments sent packet counter.
func (h *AdminHandler) RecordPacketSent() {
	h.packetsSent.Add(1)
}

// RecordRequest increments request counter.
func (h *AdminHandler) RecordRequest() {
	h.requestsReceived.Add(1)
}

// ServerInfo returns server info as JSON.
type ServerInfo struct {
	Version           string        `json:"version"`
	Uptime            time.Duration `json:"uptime"`
	ActiveConnections int32         `json:"active_connections"`
	TotalConnections  int64         `json:"total_connections"`
	NodeCount         int           `json:"node_count"`
	WatchCount        int64         `json:"watch_count"`
	LastZXID          int64         `json:"last_zxid"`
	Mode              string        `json:"mode"`
}

// GetServerInfo returns server information as a struct.
func (h *AdminHandler) GetServerInfo() ServerInfo {
	stats := h.server.Stats()
	watchCount := stats.Watches.DataWatchCount + stats.Watches.ChildWatchCount
	return ServerInfo{
		Version:           "1.0.0",
		Uptime:            time.Since(h.startTime),
		ActiveConnections: stats.Transport.ActiveConnections,
		TotalConnections:  stats.Transport.TotalConnections,
		NodeCount:         stats.Datastore.NodeCount,
		WatchCount:        watchCount,
		LastZXID:          int64(stats.Datastore.ZXID),
		Mode:              "standalone",
	}
}

// GetServerInfoJSON returns server information as JSON.
func (h *AdminHandler) GetServerInfoJSON() ([]byte, error) {
	info := h.GetServerInfo()
	return json.MarshalIndent(info, "", "  ")
}
