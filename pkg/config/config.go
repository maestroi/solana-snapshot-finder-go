package config

import "github.com/spf13/viper"

type Config struct {
	RPCAddress           string   `mapstructure:"rpc_address"`
	SnapshotPath         string   `mapstructure:"snapshot_path"`
	MinDownloadSpeed     int      `mapstructure:"min_download_speed"`
	MaxLatency           int      `mapstructure:"max_latency"`
	NumOfRetries         int      `mapstructure:"num_of_retries"`
	SleepBeforeRetry     int      `mapstructure:"sleep_before_retry"`
	Blacklist            []string `mapstructure:"blacklist"`
	PrivateRPC           bool     `mapstructure:"private_rpc"`
	WorkerCount          int      `mapstructure:"worker_count"`
	FullThreshold        int      `mapstructure:"full_threshold"` // New: Full snapshot threshold
	IncrementalThreshold int      `mapstructure:"incremental_threshold"`
	// Retry relaxation parameters
	SpeedRelaxationFactor   float64 `mapstructure:"speed_relaxation_factor"`
	LatencyRelaxationFactor float64 `mapstructure:"latency_relaxation_factor"`
	MaxRelaxationAttempts   int     `mapstructure:"max_relaxation_attempts"`
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
	viper.SetDefault("blacklist", []string{})
	viper.SetDefault("private_rpc", false)
	viper.SetDefault("worker_count", 100)
	viper.SetDefault("full_threshold", 25000)
	viper.SetDefault("incremental_threshold", 1000)
	viper.SetDefault("speed_relaxation_factor", 0.9)
	viper.SetDefault("latency_relaxation_factor", 0.9)
	viper.SetDefault("max_relaxation_attempts", 3)

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
