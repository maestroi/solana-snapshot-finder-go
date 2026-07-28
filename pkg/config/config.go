package config

import "github.com/spf13/viper"

type Config struct {
	RPCAddress           string   `mapstructure:"rpc_address"`
	RPCAddresses         []string `mapstructure:"rpc_addresses"`
	SnapshotPath         string   `mapstructure:"snapshot_path"`
	MinDownloadSpeed     int      `mapstructure:"min_download_speed"`
	MaxLatency           int      `mapstructure:"max_latency"`
	SleepBeforeRetry     int      `mapstructure:"sleep_before_retry"`
	SpeedTestSeconds     int      `mapstructure:"speed_test_seconds"`
	Denylist             []string `mapstructure:"denylist"`
	PrivateRPC           bool     `mapstructure:"private_rpc"`
	WorkerCount          int      `mapstructure:"worker_count"`
	FullThreshold        int      `mapstructure:"full_threshold"`
	IncrementalThreshold int      `mapstructure:"incremental_threshold"`
	SpeedTestCandidates  int      `mapstructure:"speed_test_candidates"`
	SpeedTestWorkers     int      `mapstructure:"speed_test_workers"`
	SpeedTestMaxBytes    int64    `mapstructure:"speed_test_max_bytes"`
	WarmStartMinNodes    int      `mapstructure:"warm_start_min_nodes"`
	// Retry relaxation parameters
	SpeedRelaxationFactor   float64 `mapstructure:"speed_relaxation_factor"`
	LatencyRelaxationFactor float64 `mapstructure:"latency_relaxation_factor"`
	MaxRelaxationAttempts   int     `mapstructure:"max_relaxation_attempts"`
	// Retry parameters
	NumOfRetries int `mapstructure:"num_of_retries"`
	// Download retry parameters
	MaxDownloadRetries int `mapstructure:"max_download_retries"`

	// Max slot filter - find snapshots at or before this slot (0 = disabled, find latest)
	MaxSlot int64 `mapstructure:"max_slot"`

	// Whitelist of preferred snapshot sources (RPC endpoints or static HTTP URLs)
	Whitelist     []string `mapstructure:"whitelist"`
	WhitelistMode string   `mapstructure:"whitelist_mode"` // "only", "additional", or "disabled"

	// Download safety - grab a matching incremental before a long full download
	DownloadIncrementalFirst bool `mapstructure:"download_incremental_first"`

	// Snapshot retention - keep at most N full snapshots (0 = unlimited)
	MaxFullSnapshots int `mapstructure:"max_full_snapshots"`
}

func LoadConfig(configPath string) (Config, error) {
	var config Config

	// Configure viper
	viper.SetConfigFile(configPath) // Use the full path to the configuration file

	// Set defaults
	viper.SetDefault("rpc_address", "https://api.mainnet-beta.solana.com")
	viper.SetDefault("snapshot_path", "./snapshots")
	viper.SetDefault("min_download_speed", 100)
	viper.SetDefault("max_latency", 200)
	viper.SetDefault("num_of_retries", 3)
	viper.SetDefault("sleep_before_retry", 5)
	viper.SetDefault("speed_test_seconds", 3)
	viper.SetDefault("denylist", []string{})
	viper.SetDefault("private_rpc", false)
	viper.SetDefault("worker_count", 100)
	viper.SetDefault("full_threshold", 100000) // Agave 3.x: ~100k slots after full
	viper.SetDefault("incremental_threshold", 1000)
	viper.SetDefault("speed_test_candidates", 30)
	viper.SetDefault("speed_test_workers", 5)
	viper.SetDefault("speed_test_max_bytes", int64(268435456))
	viper.SetDefault("warm_start_min_nodes", 3)
	viper.SetDefault("speed_relaxation_factor", 0.9)
	viper.SetDefault("latency_relaxation_factor", 0.9)
	viper.SetDefault("max_relaxation_attempts", 3)
	viper.SetDefault("max_download_retries", 3)
	viper.SetDefault("max_slot", 0) // 0 = disabled, find latest snapshots
	viper.SetDefault("whitelist", []string{})
	viper.SetDefault("whitelist_mode", "additional")
	viper.SetDefault("download_incremental_first", true)
	viper.SetDefault("max_full_snapshots", 2)

	// Read the config
	err := viper.ReadInConfig()
	if err != nil {
		return config, err
	}

	// Unmarshal the config into the struct
	err = viper.Unmarshal(&config)
	if err != nil {
		return config, err
	}

	return config, nil
}

// RPCEndpoints returns the configured RPC addresses to try in order,
// falling back to the single legacy rpc_address if rpc_addresses is unset.
func (c Config) RPCEndpoints() []string {
	if len(c.RPCAddresses) > 0 {
		return c.RPCAddresses
	}
	return []string{c.RPCAddress}
}
