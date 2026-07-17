// Package lease provides a Valkey-backed implementation of the core
// lease.Manager contract. Shard owners and worker heartbeats are stored in
// expiry-aware per-coordination-identity indexes, and owner-matching Lua
// scripts keep ownership transfers safe against concurrent workers.
package lease

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"sync"
	"time"

	valkey "github.com/valkey-io/valkey-go"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/internal/backend"
	consumerlease "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

// Manager and valkeyLease satisfy the core lease contracts.
var (
	_ consumerlease.Manager = (*Manager)(nil)
	_ consumerlease.Lease   = (*valkeyLease)(nil)
)

// Config controls how the lease manager connects and where it writes keys.
type Config = backend.LeaseConfig

// CredentialsFn supplies AUTH credentials dynamically. It is invoked on each
// connection attempt (initial dial and every reconnect), so rotated
// credentials are picked up without rebuilding the manager. An empty username
// authenticates the default user. It must be safe for concurrent use.
type CredentialsFn = backend.CredentialsFn

// Option customizes a Valkey-backed lease manager.
type Option func(*Config) error

// Manager coordinates shard ownership across consumer workers using Valkey
// (exclusive in steady state — see the lease.Manager contract for the
// ownership-transfer windows). Versioned aggregate keys are written as:
//
//	<EscapedKeyPrefix>:v2:{<base64url(coordinationIdentity)>}:lease-owners
//	<EscapedKeyPrefix>:v2:{<base64url(coordinationIdentity)>}:lease-expirations
//	<EscapedKeyPrefix>:v2:{<base64url(coordinationIdentity)>}:workers
//
// The shared hash tag routes every structure for an identity to one Redis
// Cluster slot. The consumer supplies coordinationIdentity as
// "<consumerGroup>:<canonicalStreamName>", so shard leases and heartbeats are
// isolated by group while workers in one group share the same namespace.
type Manager struct {
	client    valkey.Client
	keyPrefix string
	slots     *backend.SlotTracker
}

// NewManager creates a Manager connected to addr. It validates the resulting
// config, opens a client, and verifies connectivity with a PING bounded by the
// configured ping timeout. MaxLeases (via WithMaxLeases) bounds how many leases
// this manager will hold at once; a value <= 0 means unlimited.
func NewManager(addr string, opts ...Option) (*Manager, error) {
	cfg := backend.DefaultLeaseConfig(addr)
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	cfg, err := backend.FinalizeLeaseConfig(cfg, "valkey")
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

	return &Manager{
		client:    client,
		keyPrefix: cfg.KeyPrefix,
		slots:     backend.NewSlotTracker(cfg.MaxLeases),
	}, nil
}

// Close releases the underlying client.
func (m *Manager) Close() error {
	m.client.Close()
	return nil
}

// Acquire tries to claim a shard. It returns (nil, false, nil) when someone
// else owns the shard or the manager is already holding its maximum number of
// leases.
func (m *Manager) Acquire(ctx context.Context, streamName, shardID, owner string, ttl time.Duration) (consumerlease.Lease, bool, error) {
	keys := m.keys(streamName)
	slotKey := keys.LeaseOwners + "\x00" + shardID
	releaseSlot, ok := m.slots.Reserve(slotKey)
	if !ok {
		return nil, false, nil
	}

	resp := m.client.Do(ctx, m.client.B().Eval().Script(backend.LeaseAcquireScript).Numkeys(2).
		Key(keys.LeaseOwners, keys.LeaseExpirations).
		Arg(shardID, owner, strconv.FormatInt(ttl.Milliseconds(), 10)).Build())
	res, err := resp.ToInt64()
	if err != nil {
		releaseSlot()
		return nil, false, fmt.Errorf("acquire lease %s/%s: %w", streamName, shardID, err)
	}
	if res == 0 {
		releaseSlot()
		return nil, false, nil
	}

	return &valkeyLease{
		client:      m.client,
		ownersKey:   keys.LeaseOwners,
		expiriesKey: keys.LeaseExpirations,
		shardID:     shardID,
		owner:       owner,
		done:        releaseSlot,
	}, true, nil
}

// List returns active shard owners for a stream, keyed by shard ID.
func (m *Manager) List(ctx context.Context, streamName string) (map[string]string, error) {
	keys := m.keys(streamName)
	result, err := m.client.Do(ctx, m.client.B().Eval().Script(backend.LeaseListScript).Numkeys(2).
		Key(keys.LeaseOwners, keys.LeaseExpirations).Build()).AsStrMap()
	if err != nil {
		return nil, fmt.Errorf("list leases %s: %w", streamName, err)
	}
	return result, nil
}

// Claim tries to transfer a shard from expectedOwner to newOwner. It returns
// (nil, false, nil) when the current owner does not match expectedOwner or the
// manager is already holding its maximum number of leases.
func (m *Manager) Claim(ctx context.Context, streamName, shardID, expectedOwner, newOwner string, ttl time.Duration) (consumerlease.Lease, bool, error) {
	keys := m.keys(streamName)
	slotKey := keys.LeaseOwners + "\x00" + shardID
	releaseSlot, ok := m.slots.Reserve(slotKey)
	if !ok {
		return nil, false, nil
	}

	resp := m.client.Do(ctx, m.client.B().Eval().Script(backend.LeaseClaimScript).Numkeys(2).
		Key(keys.LeaseOwners, keys.LeaseExpirations).
		Arg(shardID, expectedOwner, strconv.FormatInt(ttl.Milliseconds(), 10), newOwner).Build())
	res, err := resp.ToInt64()
	if err != nil {
		releaseSlot()
		return nil, false, fmt.Errorf("claim lease %s/%s: %w", streamName, shardID, err)
	}
	if res == 0 {
		releaseSlot()
		return nil, false, nil
	}

	return &valkeyLease{
		client:      m.client,
		ownersKey:   keys.LeaseOwners,
		expiriesKey: keys.LeaseExpirations,
		shardID:     shardID,
		owner:       newOwner,
		done:        releaseSlot,
	}, true, nil
}

// Heartbeat marks this worker as alive for the given TTL.
func (m *Manager) Heartbeat(ctx context.Context, streamName, owner string, ttl time.Duration) error {
	key := m.keys(streamName).Workers
	if err := m.client.Do(ctx, m.client.B().Eval().Script(backend.WorkerHeartbeatScript).Numkeys(1).
		Key(key).Arg(owner, strconv.FormatInt(ttl.Milliseconds(), 10)).Build()).Error(); err != nil {
		return fmt.Errorf("heartbeat %s/%s: %w", streamName, owner, err)
	}
	return nil
}

// Workers lists currently alive owners for the stream.
func (m *Manager) Workers(ctx context.Context, streamName string) ([]string, error) {
	key := m.keys(streamName).Workers
	entries, err := m.client.Do(ctx, m.client.B().Eval().Script(backend.WorkerListScript).Numkeys(1).
		Key(key).Build()).ToArray()
	if err != nil {
		return nil, fmt.Errorf("list workers %s: %w", streamName, err)
	}

	owners := make([]string, 0, len(entries))
	for i := range entries {
		owner, err := entries[i].ToString()
		if err != nil {
			return nil, fmt.Errorf("decode workers %s: %w", streamName, err)
		}
		owners = append(owners, owner)
	}
	return owners, nil
}

func (m *Manager) keys(streamName string) backend.CoordinationKeys {
	return backend.LeaseCoordinationKeys(m.keyPrefix, streamName)
}

// valkeyLease represents ownership of one shard. The done func releases the
// SlotTracker reservation and is guarded by once so it runs at most once across
// Renew (on loss of ownership) and Release.
type valkeyLease struct {
	client      valkey.Client
	ownersKey   string
	expiriesKey string
	shardID     string
	owner       string
	done        func()

	once sync.Once
}

// Renew extends the TTL only if the caller still owns the lease. On loss of
// ownership it releases the slot reservation and returns ErrNotOwned.
func (l *valkeyLease) Renew(ctx context.Context, ttl time.Duration) error {
	resp := l.client.Do(ctx, l.client.B().Eval().Script(backend.LeaseRenewScript).Numkeys(2).
		Key(l.ownersKey, l.expiriesKey).
		Arg(l.shardID, l.owner, strconv.FormatInt(ttl.Milliseconds(), 10)).Build())
	res, err := resp.ToInt64()
	if err != nil {
		return fmt.Errorf("renew lease %s: %w", l.shardID, err)
	}
	if res == 0 {
		l.once.Do(l.done)
		return consumerlease.ErrNotOwned
	}
	return nil
}

// Release deletes the key only when owned by the caller. It always releases the
// slot reservation, and returns ErrNotOwned when the caller no longer owns the
// lease.
func (l *valkeyLease) Release(ctx context.Context) error {
	resp := l.client.Do(ctx, l.client.B().Eval().Script(backend.LeaseReleaseScript).Numkeys(2).
		Key(l.ownersKey, l.expiriesKey).Arg(l.shardID, l.owner).Build())
	res, err := resp.ToInt64()
	l.once.Do(l.done)
	if err != nil {
		return fmt.Errorf("release lease %s: %w", l.shardID, err)
	}
	if res == 0 {
		return consumerlease.ErrNotOwned
	}
	return nil
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
// later caller mutation does not affect the manager; nil is rejected.
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(cfg *Config) error {
		return backend.SetLeaseTLSConfig(cfg, tlsConfig)
	}
}

// WithAuth authenticates with static credentials. The password is required;
// an empty username authenticates the default user (password-only
// deployments). The password is never logged, and formatting the manager's
// config redacts it. Mutually exclusive with WithCredentialsProvider.
func WithAuth(username, password string) Option {
	return func(cfg *Config) error {
		return backend.SetLeaseAuth(cfg, username, password)
	}
}

// WithCredentialsProvider authenticates with dynamically supplied credentials.
// fn is invoked on each connection attempt (initial dial and every reconnect),
// so rotated credentials are picked up without rebuilding the manager; nil is
// rejected. Mutually exclusive with WithAuth.
func WithCredentialsProvider(fn CredentialsFn) Option {
	return func(cfg *Config) error {
		return backend.SetLeaseCredentialsFn(cfg, fn)
	}
}

// WithCluster configures the manager to use a Valkey cluster endpoint.
func WithCluster() Option {
	return func(cfg *Config) error {
		cfg.UseCluster = true
		return nil
	}
}

// WithKeyPrefix overrides the prefix used for lease keys.
func WithKeyPrefix(prefix string) Option {
	return func(cfg *Config) error {
		return backend.SetLeaseKeyPrefix(cfg, prefix)
	}
}

// WithPingTimeout overrides the timeout used to verify connectivity in
// NewManager.
func WithPingTimeout(timeout time.Duration) Option {
	return func(cfg *Config) error {
		return backend.SetLeasePingTimeout(cfg, timeout)
	}
}

// WithDB selects the Valkey database when connecting to a standalone node.
func WithDB(db int) Option {
	return func(cfg *Config) error {
		return backend.SetLeaseDB(cfg, db)
	}
}

// WithMaxLeases bounds how many leases this manager will hold at once. Zero
// means unlimited.
func WithMaxLeases(maxLeases int) Option {
	return func(cfg *Config) error {
		return backend.SetLeaseMaxLeases(cfg, maxLeases)
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
