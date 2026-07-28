package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/config"
)

var version string

func main() {
	// Parse command-line arguments
	configPath := flag.String("config", "./config.yaml", "Path to the configuration file directory")
	versionFlag := flag.Bool("version", false, "Show version information")
	maxSlot := flag.Int64("max-slot", 0, "Find the newest snapshot with slot <= this value (0 = disabled, find latest)")
	flag.Parse()

	// If version flag is passed, print the version and exit
	if *versionFlag {
		if version == "" {
			version = "unknown" // In case version is not set, fallback to 'unknown'
		}
		fmt.Printf("Solana Snapshot Finder, Version: %s\n", version)
		os.Exit(0)
	}

	// Load the configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Override MaxSlot from command-line flag if provided
	if *maxSlot > 0 {
		cfg.MaxSlot = *maxSlot
		log.Printf("Using max slot from command-line flag: %d", cfg.MaxSlot)
	}

	log.Println("Starting Solana Snapshot Finder...")
	log.Printf("Loaded Config: %+v", cfg)

	// Manage snapshots, including fetching the reference slot internally
	processSnapshots(cfg)
}
