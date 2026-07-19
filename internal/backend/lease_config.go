package backend

import (
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
)

// LeaseConfig describes how a Redis-family lease manager connects and where it
// writes keys. It is shared by concrete backends so their connection options
// behave identically. MaxLeases bounds how many leases one manager will hold at
// once; a value <= 0 means unlimited. The embedded connection-security fields
// (Username, Password, Credentials, TLSConfig) are set only through their
// setters so secrets and TLS configs are validated and cloned consistently.
type LeaseConfig struct {
	authConfig
	Addr        string
	UseTLS      bool
	UseCluster  bool
	DB          int
	KeyPrefix   string
	PingTimeout time.Duration
	MaxLeases   int
}

// String renders the config for logs and errors with the password redacted
// and the credentials provider reduced to a presence marker, so formatting a
// config can never leak a secret.
func (c LeaseConfig) String() string {
	return fmt.Sprintf(
		"LeaseConfig{Addr:%s Username:%s Password:%s Credentials:%s TLSConfig:%v UseTLS:%t UseCluster:%t DB:%d KeyPrefix:%s PingTimeout:%s MaxLeases:%d}",
		c.Addr, c.Username, c.redactedStaticPassword(), c.redactedCredentials(), c.TLSConfig != nil,
		c.UseTLS, c.UseCluster, c.DB, c.KeyPrefix, c.PingTimeout, c.MaxLeases,
	)
}

// GoString redacts %#v formatting the same way String redacts %v/%+v.
func (c LeaseConfig) GoString() string {
	return c.String()
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
	if err := cfg.authConfig.validate(backendName); err != nil {
		return LeaseConfig{}, err
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = defaultLeasePrefix
	}
	if cfg.PingTimeout <= 0 {
		cfg.PingTimeout = defaultPingTimeout
	}
	return cfg, nil
}

// keyPrefixEscaper escapes hash-tag delimiters in configured key prefixes
// injectively, shared by the lease and checkpoint key formats. Percent is
// escaped first by NewReplacer's single-pass matching, keeping literal "%7B"
// distinct from literal "{".
var keyPrefixEscaper = strings.NewReplacer("%", "%25", "{", "%7B", "}", "%7D")

// CoordinationKeys are the versioned aggregate keys used to coordinate one
// consumer group and stream. Identity is base64url-encoded inside a cluster
// hash tag so arbitrary Manager callers cannot inject braces and split the
// multi-key lease scripts across slots.
type CoordinationKeys struct {
	LeaseOwners      string
	LeaseExpirations string
	Workers          string
}

// LeaseCoordinationKeys returns the v2 aggregate coordination keys. All keys
// share one Redis Cluster hash tag and therefore route to the same slot.
func LeaseCoordinationKeys(prefix, coordinationIdentity string) CoordinationKeys {
	identity := base64.RawURLEncoding.EncodeToString([]byte(coordinationIdentity))
	if identity == "" {
		// Redis ignores an empty hash tag ("{}"), which would let the suffixes
		// route to different slots. A one-character token cannot collide with
		// non-empty raw base64url input, whose shortest encoding is two bytes.
		identity = "-"
	}
	// Escape prefix delimiters injectively so a custom prefix cannot introduce
	// an earlier (or empty) hash tag.
	base := fmt.Sprintf("%s:v2:{%s}", keyPrefixEscaper.Replace(prefix), identity)
	return CoordinationKeys{
		LeaseOwners:      base + ":lease-owners",
		LeaseExpirations: base + ":lease-expirations",
		Workers:          base + ":workers",
	}
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

// SetLeaseAuth records static AUTH credentials. The password is required; an
// empty username authenticates the default user (password-only deployments).
func SetLeaseAuth(cfg *LeaseConfig, username, password string) error {
	return cfg.setAuth(username, password)
}

// SetLeaseCredentialsFn records a dynamic credential provider, rejecting nil.
// Mutually exclusive with static credentials (enforced at finalize).
func SetLeaseCredentialsFn(cfg *LeaseConfig, fn CredentialsFn) error {
	return cfg.setCredentialsFn(fn)
}

// SetLeaseTLSConfig stores a clone of the caller's TLS config (nil rejected)
// and enables TLS.
func SetLeaseTLSConfig(cfg *LeaseConfig, tlsConfig *tls.Config) error {
	if err := cfg.setTLSConfig(tlsConfig); err != nil {
		return err
	}
	cfg.UseTLS = true
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

// Lua scripts run by the lease Manager. Lease owners live in a hash and their
// absolute expiration times live in a sorted set; worker expirations live in a
// second sorted set. All expiry calculations use Valkey server time. The
// scripts are inert strings here and are validated end-to-end by the Manager's
// miniredis tests.
const (
	leaseNowMilliseconds = `
local time = redis.call("time")
local now = (time[1] * 1000) + math.floor(time[2] / 1000)
`

	// LeaseAcquireScript creates a lease only when no live lease exists.
	// KEYS[1]=owners hash, KEYS[2]=expiration zset,
	// ARGV[1]=shard, ARGV[2]=owner, ARGV[3]=ttl ms.
	LeaseAcquireScript = leaseNowMilliseconds + `
local expiration = redis.call("zscore", KEYS[2], ARGV[1])
local current_owner = redis.call("hget", KEYS[1], ARGV[1])
if expiration and tonumber(expiration) > now and current_owner then
  return 0
end
redis.call("hdel", KEYS[1], ARGV[1])
redis.call("zrem", KEYS[2], ARGV[1])
redis.call("hset", KEYS[1], ARGV[1], ARGV[2])
redis.call("zadd", KEYS[2], now + tonumber(ARGV[3]), ARGV[1])
local latest = redis.call("zrevrange", KEYS[2], 0, 0, "withscores")
redis.call("pexpireat", KEYS[1], latest[2])
redis.call("pexpireat", KEYS[2], latest[2])
return 1
`

	// LeaseClaimScript transfers a live lease only if its owner matches.
	// KEYS[1]=owners hash, KEYS[2]=expiration zset,
	// ARGV[1]=shard, ARGV[2]=expected owner, ARGV[3]=ttl ms,
	// ARGV[4]=new owner.
	LeaseClaimScript = `
` + leaseNowMilliseconds + `
local expiration = redis.call("zscore", KEYS[2], ARGV[1])
local current_owner = redis.call("hget", KEYS[1], ARGV[1])
if expiration and tonumber(expiration) > now and current_owner == ARGV[2] then
  redis.call("hset", KEYS[1], ARGV[1], ARGV[4])
  redis.call("zadd", KEYS[2], now + tonumber(ARGV[3]), ARGV[1])
  local latest = redis.call("zrevrange", KEYS[2], 0, 0, "withscores")
  redis.call("pexpireat", KEYS[1], latest[2])
  redis.call("pexpireat", KEYS[2], latest[2])
  return 1
end
if not current_owner or not expiration or tonumber(expiration) <= now then
  redis.call("hdel", KEYS[1], ARGV[1])
  redis.call("zrem", KEYS[2], ARGV[1])
end
return 0
`

	// LeaseRenewScript extends the TTL only if the caller still owns the lease.
	// KEYS[1]=owners hash, KEYS[2]=expiration zset,
	// ARGV[1]=shard, ARGV[2]=owner, ARGV[3]=ttl ms.
	LeaseRenewScript = leaseNowMilliseconds + `
local expiration = redis.call("zscore", KEYS[2], ARGV[1])
local current_owner = redis.call("hget", KEYS[1], ARGV[1])
if expiration and tonumber(expiration) > now and current_owner == ARGV[2] then
  redis.call("zadd", KEYS[2], now + tonumber(ARGV[3]), ARGV[1])
  local latest = redis.call("zrevrange", KEYS[2], 0, 0, "withscores")
  redis.call("pexpireat", KEYS[1], latest[2])
  redis.call("pexpireat", KEYS[2], latest[2])
  return 1
end
if not current_owner or not expiration or tonumber(expiration) <= now then
  redis.call("hdel", KEYS[1], ARGV[1])
  redis.call("zrem", KEYS[2], ARGV[1])
end
return 0
`

	// LeaseReleaseScript deletes the lease only if the caller still owns it.
	// KEYS[1]=owners hash, KEYS[2]=expiration zset,
	// ARGV[1]=shard, ARGV[2]=owner.
	LeaseReleaseScript = leaseNowMilliseconds + `
local expiration = redis.call("zscore", KEYS[2], ARGV[1])
local current_owner = redis.call("hget", KEYS[1], ARGV[1])
if expiration and tonumber(expiration) > now and current_owner == ARGV[2] then
  redis.call("hdel", KEYS[1], ARGV[1])
  redis.call("zrem", KEYS[2], ARGV[1])
  local latest = redis.call("zrevrange", KEYS[2], 0, 0, "withscores")
  if #latest > 0 then
    redis.call("pexpireat", KEYS[1], latest[2])
    redis.call("pexpireat", KEYS[2], latest[2])
  end
  return 1
end
if not current_owner or not expiration or tonumber(expiration) <= now then
  redis.call("hdel", KEYS[1], ARGV[1])
  redis.call("zrem", KEYS[2], ARGV[1])
end
return 0
`

	// LeaseListScript returns a flat shard/owner array containing only live
	// leases and removes expired or one-sided hash/zset entries atomically.
	// KEYS[1]=owners hash, KEYS[2]=expiration zset.
	LeaseListScript = leaseNowMilliseconds + `
local result = {}
local indexed = redis.call("zrange", KEYS[2], 0, -1, "withscores")
for i = 1, #indexed, 2 do
  local shard = indexed[i]
  local expiration = tonumber(indexed[i + 1])
  local owner = redis.call("hget", KEYS[1], shard)
  if owner and expiration > now then
    result[#result + 1] = shard
    result[#result + 1] = owner
  else
    redis.call("hdel", KEYS[1], shard)
    redis.call("zrem", KEYS[2], shard)
  end
end
local owned = redis.call("hkeys", KEYS[1])
for i = 1, #owned do
  if not redis.call("zscore", KEYS[2], owned[i]) then
    redis.call("hdel", KEYS[1], owned[i])
  end
end
local latest = redis.call("zrevrange", KEYS[2], 0, 0, "withscores")
if #latest > 0 then
  redis.call("pexpireat", KEYS[1], latest[2])
  redis.call("pexpireat", KEYS[2], latest[2])
end
return result
`

	// WorkerHeartbeatScript records an absolute worker expiration using server
	// time. KEYS[1]=workers zset, ARGV[1]=owner, ARGV[2]=ttl ms.
	WorkerHeartbeatScript = leaseNowMilliseconds + `
redis.call("zadd", KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
local latest = redis.call("zrevrange", KEYS[1], 0, 0, "withscores")
redis.call("pexpireat", KEYS[1], latest[2])
return 1
`

	// WorkerListScript removes expired workers and returns only live owners.
	// KEYS[1]=workers zset.
	WorkerListScript = leaseNowMilliseconds + `
redis.call("zremrangebyscore", KEYS[1], "-inf", now)
local result = redis.call("zrange", KEYS[1], 0, -1)
local latest = redis.call("zrevrange", KEYS[1], 0, 0, "withscores")
if #latest > 0 then
  redis.call("pexpireat", KEYS[1], latest[2])
end
return result
`

	// WorkerDeregisterScript removes one worker from the live-worker set on a
	// clean shutdown and recomputes the key's expiry from the remaining workers'
	// latest score, so peers stop counting the departed worker immediately.
	// Removing an owner that is already absent is a no-op. Removing the last
	// worker deletes the key outright (Redis drops an emptied sorted set), so
	// there is no remaining expiry to recompute.
	// KEYS[1]=workers zset, ARGV[1]=owner.
	WorkerDeregisterScript = `
redis.call("zrem", KEYS[1], ARGV[1])
local latest = redis.call("zrevrange", KEYS[1], 0, 0, "withscores")
if #latest > 0 then
  redis.call("pexpireat", KEYS[1], latest[2])
end
return 1
`
)
