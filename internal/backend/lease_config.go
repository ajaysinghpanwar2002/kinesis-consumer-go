package backend

import (
	"errors"
	"fmt"
	"time"
)

// LeaseConfig describes how a Redis-family lease manager connects and where it
// writes keys. It is shared by concrete backends so their connection options
// behave identically. MaxLeases bounds how many leases one manager will hold at
// once; a value <= 0 means unlimited.
type LeaseConfig struct {
	Addr        string
	UseTLS      bool
	UseCluster  bool
	DB          int
	KeyPrefix   string
	PingTimeout time.Duration
	MaxLeases   int
}

// DefaultLeaseConfig returns a config with only the address populated.
// Remaining fields are filled in by FinalizeLeaseConfig.
func DefaultLeaseConfig(addr string) LeaseConfig {
	return LeaseConfig{Addr: addr}
}

// FinalizeLeaseConfig validates cfg and applies defaults, returning the
// completed config. backendName is used only to make error messages name the
// concrete backend (for example "valkey db must be >= 0"). MaxLeases is left
// unvalidated: a non-positive value means "unlimited" and is honored by
// SlotTracker.
func FinalizeLeaseConfig(cfg LeaseConfig, backendName string) (LeaseConfig, error) {
	if cfg.Addr == "" {
		return LeaseConfig{}, fmt.Errorf("%s address is required", backendName)
	}
	if cfg.DB < 0 {
		return LeaseConfig{}, fmt.Errorf("%s db must be >= 0", backendName)
	}
	if cfg.UseCluster && cfg.DB != 0 {
		return LeaseConfig{}, fmt.Errorf("%s db is not supported with cluster mode", backendName)
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = defaultLeasePrefix
	}
	if cfg.PingTimeout <= 0 {
		cfg.PingTimeout = defaultPingTimeout
	}
	return cfg, nil
}

// LeaseKey formats the key used to store a shard lease owner.
func LeaseKey(prefix, streamName, shardID string) string {
	return fmt.Sprintf("%s:%s:%s", prefix, streamName, shardID)
}

// WorkerKey formats the key used to record a live worker heartbeat.
func WorkerKey(prefix, streamName, owner string) string {
	return fmt.Sprintf("%s:%s:%s", prefix, streamName, owner)
}

// SetLeaseKeyPrefix overrides the lease key prefix, rejecting an empty value.
func SetLeaseKeyPrefix(cfg *LeaseConfig, prefix string) error {
	if prefix == "" {
		return errors.New("key prefix cannot be empty")
	}
	cfg.KeyPrefix = prefix
	return nil
}

// SetLeasePingTimeout overrides the ping timeout, rejecting a non-positive
// value.
func SetLeasePingTimeout(cfg *LeaseConfig, timeout time.Duration) error {
	if timeout <= 0 {
		return errors.New("ping timeout must be > 0")
	}
	cfg.PingTimeout = timeout
	return nil
}

// SetLeaseDB selects the database index, rejecting a negative value.
func SetLeaseDB(cfg *LeaseConfig, db int) error {
	if db < 0 {
		return errors.New("db must be >= 0")
	}
	cfg.DB = db
	return nil
}

// SetLeaseMaxLeases sets the maximum number of concurrently held leases,
// rejecting a negative value. Zero is allowed and means unlimited.
func SetLeaseMaxLeases(cfg *LeaseConfig, maxLeases int) error {
	if maxLeases < 0 {
		return errors.New("max leases must be >= 0")
	}
	cfg.MaxLeases = maxLeases
	return nil
}

// Lua scripts run by the lease Manager. Each checks that the caller still owns
// the key (its value equals the caller's owner token) before mutating it, which
// keeps claim/renew/release safe against concurrent owners. They are inert
// strings here and are validated end-to-end by the Manager's miniredis tests.
const (
	// LeaseClaimScript transfers a lease to a new owner only if the current
	// value equals the expected owner. KEYS[1]=lease key,
	// ARGV[1]=expected owner, ARGV[2]=ttl ms, ARGV[3]=new owner.
	LeaseClaimScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  redis.call("psetex", KEYS[1], ARGV[2], ARGV[3])
  return 1
end
return 0
`

	// LeaseRenewScript extends the TTL only if the caller still owns the lease.
	// KEYS[1]=lease key, ARGV[1]=owner, ARGV[2]=ttl ms.
	LeaseRenewScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
`

	// LeaseReleaseScript deletes the lease only if the caller still owns it.
	// KEYS[1]=lease key, ARGV[1]=owner.
	LeaseReleaseScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
end
return 0
`
)
