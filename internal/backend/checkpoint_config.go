// Package backend holds shared connection configuration and key-formatting
// helpers for Redis-family checkpoint stores and lease managers (for example
// Valkey and Redis). It is dependency-free so backend packages can compose it
// without pulling any client library into the core module.
package backend

import (
	"crypto/tls"
	"encoding/base64"
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
// options behave identically. The embedded connection-security fields
// (Username, Password, Credentials, TLSConfig) are set only through their
// setters so secrets and TLS configs are validated and cloned consistently.
type CheckpointConfig struct {
	authConfig
	Addr        string
	UseTLS      bool
	UseCluster  bool
	DB          int
	KeyPrefix   string
	PingTimeout time.Duration
	LeasePrefix string
}

// String renders the config for logs and errors with the password redacted
// and the credentials provider reduced to a presence marker, so formatting a
// config can never leak a secret.
func (c CheckpointConfig) String() string {
	return fmt.Sprintf(
		"CheckpointConfig{Addr:%s Username:%s Password:%s Credentials:%s TLSConfig:%v UseTLS:%t UseCluster:%t DB:%d KeyPrefix:%s PingTimeout:%s LeasePrefix:%s}",
		c.Addr, c.Username, c.redactedStaticPassword(), c.redactedCredentials(), c.TLSConfig != nil,
		c.UseTLS, c.UseCluster, c.DB, c.KeyPrefix, c.PingTimeout, c.LeasePrefix,
	)
}

// GoString redacts %#v formatting the same way String redacts %v/%+v.
func (c CheckpointConfig) GoString() string {
	return c.String()
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
	if err := cfg.authConfig.validate(backendName); err != nil {
		return CheckpointConfig{}, err
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

// CheckpointKey formats the versioned key used to store a shard checkpoint:
//
//	<escapedPrefix>:v2:<enc(streamName)>:<enc(shardID)>
//
// where enc is unpadded base64url. The encoding is injective — the base64url
// alphabet excludes the ":" delimiter, so distinct (stream, shard) pairs can
// never collide even when a segment itself contains ":" — and the v2 segment
// anchors future format migrations, mirroring the lease layer's encoding.
// The consumer passes its coordination identity ("<group>:<stream>") as
// streamName, so the encoded segment equals the lease keys' identity64.
// Unlike lease keys there is no cluster hash tag: every checkpoint operation
// is single-key, and untagged per-shard keys stay spread across cluster
// slots.
func CheckpointKey(prefix, streamName, shardID string) string {
	return fmt.Sprintf("%s:v2:%s:%s",
		keyPrefixEscaper.Replace(prefix),
		base64.RawURLEncoding.EncodeToString([]byte(streamName)),
		base64.RawURLEncoding.EncodeToString([]byte(shardID)),
	)
}

// DefaultLeasePrefix derives a lease prefix from a checkpoint prefix. The
// default (or empty) checkpoint prefix maps to the shared standalone lease
// default, so a store-provided manager and a default standalone manager
// coordinate in the same namespace — different defaults would let two such
// workers each acquire every shard (silent dual processing). A custom
// checkpoint prefix derives `<prefix>-lease` so lease keys stay adjacent to
// checkpoint keys under prefix-based tenant isolation; standalone managers in
// such deployments must be given the matching prefix explicitly.
func DefaultLeasePrefix(checkpointPrefix string) string {
	if checkpointPrefix == "" || checkpointPrefix == defaultCheckpointKeyPrefix {
		return defaultLeasePrefix
	}
	return checkpointPrefix + "-lease"
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

// SetCheckpointAuth records static AUTH credentials. The password is
// required; an empty username authenticates the default user
// (password-only deployments).
func SetCheckpointAuth(cfg *CheckpointConfig, username, password string) error {
	return cfg.setAuth(username, password)
}

// SetCheckpointCredentialsFn records a dynamic credential provider, rejecting
// nil. Mutually exclusive with static credentials (enforced at finalize).
func SetCheckpointCredentialsFn(cfg *CheckpointConfig, fn CredentialsFn) error {
	return cfg.setCredentialsFn(fn)
}

// SetCheckpointTLSConfig stores a clone of the caller's TLS config (nil
// rejected) and enables TLS.
func SetCheckpointTLSConfig(cfg *CheckpointConfig, tlsConfig *tls.Config) error {
	if err := cfg.setTLSConfig(tlsConfig); err != nil {
		return err
	}
	cfg.UseTLS = true
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

// CheckpointSaveScript applies a checkpoint save only when it advances the
// stored value, per the checkpoint.Store contract: first write wins on a
// missing key; an equal value is an idempotent no-op; a completed
// (prefix-marked) value is terminal; a new completed value always advances;
// otherwise values compare as Kinesis sequence numbers — unsigned decimal
// strings without leading zeros, so longer is greater and equal lengths
// compare lexicographically (the numbers exceed Lua's 53-bit numeric
// precision, so tonumber would corrupt the order). Stale writes return 0 and
// change nothing. It must stay in lockstep with the memory store's
// checkpointAdvances. KEYS[1]=checkpoint key, ARGV[1]=new value,
// ARGV[2]=completed prefix.
const CheckpointSaveScript = `
local cur = redis.call("get", KEYS[1])
if cur == false then
  redis.call("set", KEYS[1], ARGV[1])
  return 1
end
if cur == ARGV[1] then
  return 0
end
if string.sub(cur, 1, #ARGV[2]) == ARGV[2] then
  return 0
end
if string.sub(ARGV[1], 1, #ARGV[2]) == ARGV[2] then
  redis.call("set", KEYS[1], ARGV[1])
  return 1
end
if #ARGV[1] > #cur or (#ARGV[1] == #cur and ARGV[1] > cur) then
  redis.call("set", KEYS[1], ARGV[1])
  return 1
end
return 0
`
