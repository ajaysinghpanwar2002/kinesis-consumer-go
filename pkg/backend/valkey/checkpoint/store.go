// Package checkpoint provides a Valkey-backed implementation of the core
// checkpoint.Store contract. Checkpoints are stored as plain string values so
// sequence numbers and SHARD_END markers persist verbatim; saves are
// advance-only (atomic compare-and-set), so a stale writer can neither
// regress a checkpoint nor overwrite a completion marker.
package checkpoint

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	valkey "github.com/valkey-io/valkey-go"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/internal/backend"
	valkeylease "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/lease"
	corecheckpoint "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	consumerlease "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

// Store satisfies the core checkpoint.Store contract and can provide a matching
// Valkey-backed lease manager through lease.Provider.
var (
	_ corecheckpoint.Store   = (*Store)(nil)
	_ consumerlease.Provider = (*Store)(nil)
)

// Config controls how the checkpoint store connects and where it writes keys.
type Config = backend.CheckpointConfig

// CredentialsFn supplies AUTH credentials dynamically. It is invoked on each
// connection attempt (initial dial and every reconnect), so rotated
// credentials are picked up without rebuilding the store. An empty username
// authenticates the default user. It must be safe for concurrent use.
type CredentialsFn = backend.CredentialsFn

// Option customizes a Valkey-backed checkpoint store.
type Option func(*Config) error

// Store persists per-shard checkpoints in Valkey.
type Store struct {
	client    valkey.Client
	keyPrefix string
	cfg       Config
}

// New creates a Store connected to addr. Keys are written in the versioned
// injective format:
//
//	<escapedKeyPrefix>:v2:<enc(coordinationIdentity)>:<enc(shardID)>
//
// where enc is unpadded base64url and hash-tag delimiters in the prefix are
// escaped, mirroring the lease key encoding. The consumer supplies
// coordinationIdentity as "<consumerGroup>:<canonicalStreamName>", so the
// encoded identity segment matches the lease keys' identity64.
//
// New validates the resulting config, opens a client, and verifies
// connectivity with a PING bounded by the configured ping timeout.
func New(addr string, opts ...Option) (*Store, error) {
	cfg := backend.DefaultCheckpointConfig(addr)
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	cfg, err := backend.FinalizeCheckpointConfig(cfg, "valkey")
	if err != nil {
		return nil, err
	}

	client, err := newClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("connect valkey: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.PingTimeout)
	defer cancel()
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping valkey: %w", err)
	}

	return &Store{
		client:    client,
		keyPrefix: cfg.KeyPrefix,
		cfg:       cfg,
	}, nil
}

// Close releases the underlying client.
func (s *Store) Close() error {
	s.client.Close()
	return nil
}

// Get reads the last checkpoint value for a shard, returning ("", nil) when no
// checkpoint exists.
func (s *Store) Get(ctx context.Context, streamName, shardID string) (string, error) {
	key := s.key(streamName, shardID)
	resp := s.client.Do(ctx, s.client.B().Get().Key(key).Build())
	if err := resp.Error(); err != nil {
		if isNil(err) {
			return "", nil
		}
		return "", fmt.Errorf("get checkpoint %s/%s: %w", streamName, shardID, err)
	}

	msg, err := resp.ToMessage()
	if err != nil {
		return "", fmt.Errorf("get checkpoint %s/%s: %w", streamName, shardID, err)
	}
	if msg.IsNil() {
		return "", nil
	}
	val, err := msg.ToString()
	if err != nil {
		return "", fmt.Errorf("parse checkpoint %s/%s: %w", streamName, shardID, err)
	}
	return val, nil
}

// Save stores the sequence number (or SHARD_END marker) for a shard verbatim,
// but only when it advances the checkpoint: a stale or duplicate value is
// silently discarded and a completed (SHARD_END-prefixed) value is never
// overwritten, per the checkpoint.Store contract. The advance-only
// compare-and-set runs atomically as a Lua script.
func (s *Store) Save(ctx context.Context, streamName, shardID, sequenceNumber string) error {
	key := s.key(streamName, shardID)
	cmd := s.client.B().Eval().Script(backend.CheckpointSaveScript).Numkeys(1).Key(key).
		Arg(sequenceNumber).Arg(corecheckpoint.CompletedPrefix).Build()
	if err := s.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("save checkpoint %s/%s: %w", streamName, shardID, err)
	}
	return nil
}

// Delete removes the checkpoint for a shard. Deleting a missing key is a no-op.
func (s *Store) Delete(ctx context.Context, streamName, shardID string) error {
	key := s.key(streamName, shardID)
	if err := s.client.Do(ctx, s.client.B().Del().Key(key).Build()).Error(); err != nil {
		return fmt.Errorf("delete checkpoint %s/%s: %w", streamName, shardID, err)
	}
	return nil
}

// LeaseManager constructs a Valkey-backed lease manager from the store's
// connection config, writing lease keys under the store's lease prefix. It
// satisfies lease.Provider, so a consumer configured with only this store
// acquires shard leasing automatically. The returned manager owns a separate
// Valkey client and pings on construction. When the consumer auto-creates the
// manager through lease.Provider it owns the manager and releases it via
// Consumer.Close; callers invoking this method directly must Close the
// manager themselves.
func (s *Store) LeaseManager() (consumerlease.Manager, error) {
	opts := []valkeylease.Option{
		valkeylease.WithKeyPrefix(s.cfg.LeasePrefix),
		valkeylease.WithPingTimeout(s.cfg.PingTimeout),
	}
	// Connection security propagates so a secured store yields an equally
	// secured auto-created manager. CloneTLSConfig hands the manager its own
	// copy; WithTLS stays for the config-less TLS case.
	if tlsConfig := s.cfg.CloneTLSConfig(); tlsConfig != nil {
		opts = append(opts, valkeylease.WithTLSConfig(tlsConfig))
	} else if s.cfg.UseTLS {
		opts = append(opts, valkeylease.WithTLS())
	}
	if s.cfg.Credentials != nil {
		opts = append(opts, valkeylease.WithCredentialsProvider(s.cfg.Credentials))
	} else if s.cfg.Password != "" {
		opts = append(opts, valkeylease.WithAuth(s.cfg.Username, s.cfg.Password))
	}
	if s.cfg.UseCluster {
		opts = append(opts, valkeylease.WithCluster())
	} else {
		opts = append(opts, valkeylease.WithDB(s.cfg.DB))
	}
	return valkeylease.NewManager(s.cfg.Addr, opts...)
}

func (s *Store) key(streamName, shardID string) string {
	return backend.CheckpointKey(s.keyPrefix, streamName, shardID)
}

// isNil reports whether err represents a missing key rather than a real
// failure.
func isNil(err error) bool {
	if errors.Is(err, valkey.Nil) {
		return true
	}
	if ve, ok := valkey.IsValkeyErr(err); ok && ve.IsNil() {
		return true
	}
	return false
}

// WithTLS enables TLS when connecting to Valkey.
func WithTLS() Option {
	return func(cfg *Config) error {
		cfg.UseTLS = true
		return nil
	}
}

// WithTLSConfig enables TLS using the caller's TLS configuration (for example
// a custom RootCAs pool or ServerName). The config is cloned immediately, so
// later caller mutation does not affect the store; nil is rejected.
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(cfg *Config) error {
		return backend.SetCheckpointTLSConfig(cfg, tlsConfig)
	}
}

// WithAuth authenticates with static credentials. The password is required;
// an empty username authenticates the default user (password-only
// deployments). The password is never logged, and formatting the store's
// config redacts it. Mutually exclusive with WithCredentialsProvider.
func WithAuth(username, password string) Option {
	return func(cfg *Config) error {
		return backend.SetCheckpointAuth(cfg, username, password)
	}
}

// WithCredentialsProvider authenticates with dynamically supplied credentials.
// fn is invoked on each connection attempt (initial dial and every reconnect),
// so rotated credentials are picked up without rebuilding the store; nil is
// rejected. Mutually exclusive with WithAuth.
func WithCredentialsProvider(fn CredentialsFn) Option {
	return func(cfg *Config) error {
		return backend.SetCheckpointCredentialsFn(cfg, fn)
	}
}

// WithCluster configures the store to use a Valkey cluster endpoint.
func WithCluster() Option {
	return func(cfg *Config) error {
		cfg.UseCluster = true
		return nil
	}
}

// WithKeyPrefix overrides the prefix used for checkpoint keys.
func WithKeyPrefix(prefix string) Option {
	return func(cfg *Config) error {
		return backend.SetCheckpointKeyPrefix(cfg, prefix)
	}
}

// WithLeasePrefix overrides the prefix used for lease keys created by
// LeaseManager. When unset, the default checkpoint prefix maps to the shared
// standalone lease default ("kinesis-lease"), and a custom checkpoint prefix
// derives an adjacent lease prefix (for example "custom-lease").
func WithLeasePrefix(prefix string) Option {
	return func(cfg *Config) error {
		return backend.SetCheckpointLeasePrefix(cfg, prefix)
	}
}

// WithPingTimeout overrides the timeout used to verify connectivity in New.
func WithPingTimeout(timeout time.Duration) Option {
	return func(cfg *Config) error {
		return backend.SetCheckpointPingTimeout(cfg, timeout)
	}
}

// WithDB selects the Valkey database when connecting to a standalone node.
func WithDB(db int) Option {
	return func(cfg *Config) error {
		return backend.SetCheckpointDB(cfg, db)
	}
}

func newClient(cfg Config) (valkey.Client, error) {
	// CloneTLSConfig hands out a private copy (already cloned once at the
	// option boundary), so no two clients share a mutable tls.Config.
	tlsConfig := cfg.CloneTLSConfig()
	if cfg.UseTLS && tlsConfig == nil {
		tlsConfig = &tls.Config{}
	}

	opts := valkey.ClientOption{
		InitAddress:       []string{cfg.Addr},
		TLSConfig:         tlsConfig,
		Username:          cfg.Username,
		Password:          cfg.Password,
		ForceSingleClient: !cfg.UseCluster,
		DisableCache:      true,
	}
	if cfg.Credentials != nil {
		fn := cfg.Credentials
		opts.AuthCredentialsFn = func(valkey.AuthCredentialsContext) (valkey.AuthCredentials, error) {
			username, password, err := fn()
			if err != nil {
				return valkey.AuthCredentials{}, err
			}
			return valkey.AuthCredentials{Username: username, Password: password}, nil
		}
	}
	if !cfg.UseCluster {
		opts.SelectDB = cfg.DB
	}
	return valkey.NewClient(opts)
}
