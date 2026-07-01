// Package backend holds shared connection configuration and key-formatting
// helpers for Redis-family checkpoint stores and lease managers (for example
// Valkey and Redis). It is dependency-free so backend packages can compose it
// without pulling any client library into the core module.
package backend

import (
	"errors"
	"fmt"
	"time"
)

// defaultCheckpointKeyPrefix is used when a checkpoint config leaves KeyPrefix
// empty.
const defaultCheckpointKeyPrefix = "kinesis-checkpoint"

// defaultLeasePrefix is used when neither a checkpoint prefix nor an explicit
// lease prefix is provided.
const defaultLeasePrefix = "kinesis-lease"

// defaultPingTimeout is applied when a checkpoint config leaves PingTimeout
// unset or non-positive.
const defaultPingTimeout = 5 * time.Second

// CheckpointConfig describes how a Redis-family checkpoint store connects and
// where it writes keys. It is shared by concrete backends so their connection
// options behave identically.
type CheckpointConfig struct {
	Addr        string
	UseTLS      bool
	UseCluster  bool
	DB          int
	KeyPrefix   string
	PingTimeout time.Duration
	LeasePrefix string
}

// DefaultCheckpointConfig returns a config with only the address populated.
// Remaining fields are filled in by FinalizeCheckpointConfig.
func DefaultCheckpointConfig(addr string) CheckpointConfig {
	return CheckpointConfig{Addr: addr}
}

// FinalizeCheckpointConfig validates cfg and applies defaults, returning the
// completed config. backendName is used only to make error messages name the
// concrete backend (for example "valkey db must be >= 0").
func FinalizeCheckpointConfig(cfg CheckpointConfig, backendName string) (CheckpointConfig, error) {
	if cfg.Addr == "" {
		return CheckpointConfig{}, fmt.Errorf("%s address is required", backendName)
	}
	if cfg.DB < 0 {
		return CheckpointConfig{}, fmt.Errorf("%s db must be >= 0", backendName)
	}
	if cfg.UseCluster && cfg.DB != 0 {
		return CheckpointConfig{}, fmt.Errorf("%s db is not supported with cluster mode", backendName)
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = defaultCheckpointKeyPrefix
	}
	if cfg.LeasePrefix == "" {
		cfg.LeasePrefix = DefaultLeasePrefix(cfg.KeyPrefix)
	}
	if cfg.PingTimeout <= 0 {
		cfg.PingTimeout = defaultPingTimeout
	}
	return cfg, nil
}

// CheckpointKey formats the key used to store a shard checkpoint.
func CheckpointKey(prefix, streamName, shardID string) string {
	return fmt.Sprintf("%s:%s:%s", prefix, streamName, shardID)
}

// DefaultLeasePrefix derives a lease prefix from a checkpoint prefix so lease
// keys stay adjacent to checkpoint keys. An empty checkpoint prefix falls back
// to the standalone lease default.
func DefaultLeasePrefix(checkpointPrefix string) string {
	if checkpointPrefix != "" {
		return checkpointPrefix + "-lease"
	}
	return defaultLeasePrefix
}

// SetCheckpointKeyPrefix overrides the checkpoint key prefix, rejecting an
// empty value.
func SetCheckpointKeyPrefix(cfg *CheckpointConfig, prefix string) error {
	if prefix == "" {
		return errors.New("key prefix cannot be empty")
	}
	cfg.KeyPrefix = prefix
	return nil
}

// SetCheckpointLeasePrefix overrides the lease key prefix, rejecting an empty
// value.
func SetCheckpointLeasePrefix(cfg *CheckpointConfig, prefix string) error {
	if prefix == "" {
		return errors.New("lease prefix cannot be empty")
	}
	cfg.LeasePrefix = prefix
	return nil
}

// SetCheckpointPingTimeout overrides the ping timeout, rejecting a
// non-positive value.
func SetCheckpointPingTimeout(cfg *CheckpointConfig, timeout time.Duration) error {
	if timeout <= 0 {
		return errors.New("ping timeout must be > 0")
	}
	cfg.PingTimeout = timeout
	return nil
}

// SetCheckpointDB selects the database index, rejecting a negative value.
func SetCheckpointDB(cfg *CheckpointConfig, db int) error {
	if db < 0 {
		return errors.New("db must be >= 0")
	}
	cfg.DB = db
	return nil
}
