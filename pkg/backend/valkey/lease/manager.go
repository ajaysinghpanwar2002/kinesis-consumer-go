// Package lease provides a Valkey-backed implementation of the core
// lease.Manager contract. Shard ownership is stored as plain string values
// (the owner token) with a TTL, and claim/renew/release use owner-matching Lua
// scripts so ownership transfers stay safe against concurrent workers.
package lease

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	valkey "github.com/valkey-io/valkey-go"

	"github.com/pratilipi/kinesis-consumer-go/internal/backend"
	consumerlease "github.com/pratilipi/kinesis-consumer-go/pkg/lease"
)

// Manager and valkeyLease satisfy the core lease contracts.
var (
	_ consumerlease.Manager = (*Manager)(nil)
	_ consumerlease.Lease   = (*valkeyLease)(nil)
)

// Config controls how the lease manager connects and where it writes keys.
type Config = backend.LeaseConfig

// Option customizes a Valkey-backed lease manager.
type Option func(*Config) error

// Manager coordinates shard ownership across consumer workers using Valkey
// (exclusive in steady state — see the lease.Manager contract for the
// ownership-transfer windows). Lease keys are written as:
//
//	<KeyPrefix>:<streamName>:<shardID>
//
// and worker heartbeat keys as:
//
//	<KeyPrefix>-worker:<streamName>:<owner>
type Manager struct {
	client     valkey.Client
	keyPrefix  string
	workPrefix string
	slots      *backend.SlotTracker
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
		client:     client,
		keyPrefix:  cfg.KeyPrefix,
		workPrefix: cfg.KeyPrefix + "-worker",
		slots:      backend.NewSlotTracker(cfg.MaxLeases),
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
	key := m.key(streamName, shardID)
	releaseSlot, ok := m.slots.Reserve(key)
	if !ok {
		return nil, false, nil
	}

	resp := m.client.Do(ctx, m.client.B().Set().Key(key).Value(owner).Nx().PxMilliseconds(ttl.Milliseconds()).Build())
	if err := resp.Error(); err != nil {
		releaseSlot()
		if isNil(err) {
			// SET NX did not set the key: the shard is already owned.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("setnx %s: %w", key, err)
	}

	return &valkeyLease{
		client: m.client,
		key:    key,
		owner:  owner,
		done:   releaseSlot,
	}, true, nil
}

// List returns active shard owners for a stream, keyed by shard ID.
func (m *Manager) List(ctx context.Context, streamName string) (map[string]string, error) {
	pattern := fmt.Sprintf("%s:%s:*", m.keyPrefix, streamName)
	keys, err := m.scanKeys(ctx, pattern)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string, len(keys))
	prefix := fmt.Sprintf("%s:%s:", m.keyPrefix, streamName)
	for _, key := range keys {
		resp := m.client.Do(ctx, m.client.B().Get().Key(key).Build())
		owner, err := resp.ToString()
		if err != nil {
			if isNil(err) {
				continue
			}
			return nil, fmt.Errorf("get %s: %w", key, err)
		}
		shardID := strings.TrimPrefix(key, prefix)
		if shardID == "" {
			continue
		}
		result[shardID] = owner
	}
	return result, nil
}

// Claim tries to transfer a shard from expectedOwner to newOwner. It returns
// (nil, false, nil) when the current owner does not match expectedOwner or the
// manager is already holding its maximum number of leases.
func (m *Manager) Claim(ctx context.Context, streamName, shardID, expectedOwner, newOwner string, ttl time.Duration) (consumerlease.Lease, bool, error) {
	key := m.key(streamName, shardID)
	releaseSlot, ok := m.slots.Reserve(key)
	if !ok {
		return nil, false, nil
	}

	resp := m.client.Do(ctx, m.client.B().Eval().Script(backend.LeaseClaimScript).Numkeys(1).Key(key).Arg(expectedOwner).Arg(strconv.FormatInt(ttl.Milliseconds(), 10)).Arg(newOwner).Build())
	res, err := resp.ToInt64()
	if err != nil {
		releaseSlot()
		return nil, false, fmt.Errorf("claim lease %s: %w", key, err)
	}
	if res == 0 {
		releaseSlot()
		return nil, false, nil
	}

	return &valkeyLease{
		client: m.client,
		key:    key,
		owner:  newOwner,
		done:   releaseSlot,
	}, true, nil
}

// Heartbeat marks this worker as alive for the given TTL.
func (m *Manager) Heartbeat(ctx context.Context, streamName, owner string, ttl time.Duration) error {
	key := m.workerKey(streamName, owner)
	if err := m.client.Do(ctx, m.client.B().Set().Key(key).Value(owner).PxMilliseconds(ttl.Milliseconds()).Build()).Error(); err != nil {
		return fmt.Errorf("heartbeat %s: %w", key, err)
	}
	return nil
}

// Workers lists currently alive owners for the stream.
func (m *Manager) Workers(ctx context.Context, streamName string) ([]string, error) {
	pattern := fmt.Sprintf("%s:%s:*", m.workPrefix, streamName)
	keys, err := m.scanKeys(ctx, pattern)
	if err != nil {
		return nil, err
	}

	prefix := fmt.Sprintf("%s:%s:", m.workPrefix, streamName)
	owners := make([]string, 0, len(keys))
	for _, key := range keys {
		owner := strings.TrimPrefix(key, prefix)
		if owner != "" {
			owners = append(owners, owner)
		}
	}
	return owners, nil
}

func (m *Manager) key(streamName, shardID string) string {
	return backend.LeaseKey(m.keyPrefix, streamName, shardID)
}

func (m *Manager) workerKey(streamName, owner string) string {
	return backend.WorkerKey(m.workPrefix, streamName, owner)
}

// scanKeys returns all keys matching pattern across every node the client
// knows about, de-duplicated.
//
// A keyless SCAN issued to a cluster client is routed to a single arbitrary
// node: valkey-go picks the first connection it iterates for a command with no
// key (the InitSlot branch of clusterClient._pick), so a cluster SCAN only
// ever walks that one node's slice of the keyspace. Lease and worker keys are
// spread across all primaries by slot, so under WithCluster() a single-node
// SCAN makes List/Workers return per-call-varying subsets — phantom dead
// workers, phantom unowned shards, nondeterministic claim churn. To see the
// whole keyspace we fan the SCAN out across each node returned by Nodes() and
// union the results.
//
// In standalone mode Nodes() returns just the one client, so this reduces to a
// single cursor walk. Keys are de-duplicated because a cluster's Nodes() also
// includes replicas, which reply with the same keys as their primary; the
// primary is always in the set, so unioning never misses a key, and a lagging
// replica can at worst surface a slightly stale key (within the
// eventual-consistency tolerance List/Workers already carry).
func (m *Manager) scanKeys(ctx context.Context, pattern string) ([]string, error) {
	var keys []string
	seen := make(map[string]struct{})
	for _, node := range m.client.Nodes() {
		if err := scanNodeKeys(ctx, node, pattern, func(key string) {
			if _, ok := seen[key]; ok {
				return
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

// scanNodeKeys walks the SCAN cursor on a single node, invoking emit for each
// key returned.
func scanNodeKeys(ctx context.Context, node valkey.Client, pattern string, emit func(string)) error {
	var cursor uint64
	for {
		resp := node.Do(ctx, node.B().Scan().Cursor(cursor).Match(pattern).Count(100).Build())
		entry, err := resp.AsScanEntry()
		if err != nil {
			return fmt.Errorf("scan %s: %w", pattern, err)
		}
		for _, key := range entry.Elements {
			emit(key)
		}
		cursor = entry.Cursor
		if cursor == 0 {
			return nil
		}
	}
}

// valkeyLease represents ownership of one shard. The done func releases the
// SlotTracker reservation and is guarded by once so it runs at most once across
// Renew (on loss of ownership) and Release.
type valkeyLease struct {
	client valkey.Client
	key    string
	owner  string
	done   func()

	once sync.Once
}

// Renew extends the TTL only if the caller still owns the lease. On loss of
// ownership it releases the slot reservation and returns ErrNotOwned.
func (l *valkeyLease) Renew(ctx context.Context, ttl time.Duration) error {
	resp := l.client.Do(ctx, l.client.B().Eval().Script(backend.LeaseRenewScript).Numkeys(1).Key(l.key).Arg(l.owner).Arg(strconv.FormatInt(ttl.Milliseconds(), 10)).Build())
	res, err := resp.ToInt64()
	if err != nil {
		return fmt.Errorf("renew lease %s: %w", l.key, err)
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
	resp := l.client.Do(ctx, l.client.B().Eval().Script(backend.LeaseReleaseScript).Numkeys(1).Key(l.key).Arg(l.owner).Build())
	res, err := resp.ToInt64()
	l.once.Do(l.done)
	if err != nil {
		return fmt.Errorf("release lease %s: %w", l.key, err)
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

func newClient(cfg Config) (valkey.Client, error) {
	var tlsConfig *tls.Config
	if cfg.UseTLS {
		tlsConfig = &tls.Config{}
	}

	opts := valkey.ClientOption{
		InitAddress:       []string{cfg.Addr},
		TLSConfig:         tlsConfig,
		ForceSingleClient: !cfg.UseCluster,
		DisableCache:      true,
	}
	if !cfg.UseCluster {
		opts.SelectDB = cfg.DB
	}
	return valkey.NewClient(opts)
}
