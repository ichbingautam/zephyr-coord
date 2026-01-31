// ZephyrCoord - A ZooKeeper-compatible coordination service
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ichbingautam/zephyr-coord/internal/server"
)

var (
	version   = "1.0.0"
	buildTime = "unknown"
)

func main() {
	// Command-line flags
	var (
		configFile  = flag.String("config", "", "path to configuration file")
		dataDir     = flag.String("dataDir", "./zephyr-data", "data directory")
		listenAddr  = flag.String("listen", ":2181", "client listen address")
		maxConns    = flag.Int("maxConnections", 10000, "maximum client connections")
		showVersion = flag.Bool("version", false, "show version and exit")
		showHelp    = flag.Bool("help", false, "show help")
	)
	flag.Parse()

	if *showHelp {
		printUsage()
		os.Exit(0)
	}

	if *showVersion {
		fmt.Printf("ZephyrCoord version %s (built %s)\n", version, buildTime)
		os.Exit(0)
	}

	// Load configuration
	config := loadConfig(*configFile, *dataDir, *listenAddr, *maxConns)

	// Print banner
	printBanner()
	log.Printf("Starting ZephyrCoord on %s", config.Transport.ListenAddr)
	log.Printf("Data directory: %s", config.DataDir)

	// Create server
	srv, err := server.NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Start server
	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Wait for ready
	if err := srv.WaitForReady(5 * time.Second); err != nil {
		log.Fatalf("Server failed to become ready: %v", err)
	}

	log.Printf("ZephyrCoord is ready, accepting connections on %s", srv.Addr())

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")

	if err := srv.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}

	log.Println("ZephyrCoord stopped")
}

func loadConfig(configFile, dataDir, listenAddr string, maxConns int) server.ServerConfig {
	config := server.DefaultServerConfig(dataDir)
	config.Transport.ListenAddr = listenAddr
	config.Transport.MaxConnections = maxConns

	// TODO: Load from config file if specified
	if configFile != "" {
		log.Printf("Config file support: %s (not yet implemented)", configFile)
	}

	return config
}

func printBanner() {
	banner := `
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║   ███████╗███████╗██████╗ ██╗  ██╗██╗   ██╗██████╗       ║
║   ╚══███╔╝██╔════╝██╔══██╗██║  ██║╚██╗ ██╔╝██╔══██╗      ║
║     ███╔╝ █████╗  ██████╔╝███████║ ╚████╔╝ ██████╔╝      ║
║    ███╔╝  ██╔══╝  ██╔═══╝ ██╔══██║  ╚██╔╝  ██╔══██╗      ║
║   ███████╗███████╗██║     ██║  ██║   ██║   ██║  ██║      ║
║   ╚══════╝╚══════╝╚═╝     ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝      ║
║                                                           ║
║               Coordination Service v1.0.0                 ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
`
	fmt.Println(banner)
}

func printUsage() {
	fmt.Println("ZephyrCoord - A ZooKeeper-compatible coordination service")
	fmt.Println()
	fmt.Println("Usage: zephyr-coord [options]")
	fmt.Println()
	fmt.Println("Options:")
	flag.PrintDefaults()
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  zephyr-coord                          # Start with defaults")
	fmt.Println("  zephyr-coord -listen :2181            # Specify port")
	fmt.Println("  zephyr-coord -dataDir /var/zephyr     # Specify data directory")
	fmt.Println()
}
