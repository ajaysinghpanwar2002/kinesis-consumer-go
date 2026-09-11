package lease

import (
	"context"
	"strings"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/internal/backend"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/internal/layout"
	core "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	valkey "github.com/valkey-io/valkey-go"
)

var _ core.FencedLease = (*valkeyLease)(nil)

// ErrIncompatibleLayout reports a namespace containing an older layout.
var ErrIncompatibleLayout = layout.ErrIncompatible

// OwnershipScript is prepended to scripts run through Binding. The first three
// keys and arguments are reserved for the lease identity.
const OwnershipScript = `
local time = redis.call("time")
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local expiry = redis.call("zscore", KEYS[2], ARGV[1])
if not expiry or tonumber(expiry) <= now or
 redis.call("hget", KEYS[1], ARGV[1]) ~= ARGV[2] or
 redis.call("hget", KEYS[3], ARGV[1]) ~= ARGV[3] then
 return redis.error_reply("NOTOWNED lease is no longer current")
end
`

var validateScript = valkey.NewLuaScript(OwnershipScript + "return {'ok'}")

func (l *valkeyLease) Generation() string { return l.generation }
func (l *valkeyLease) markInvalid() {
	l.invalid.Store(true)
	l.once.Do(l.done)
}
func (l *valkeyLease) Invalidate() {
	_ = l.mu.Lock(context.Background())
	defer l.mu.Unlock()
	l.markInvalid()
}
func (l *valkeyLease) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.invalid.Load() {
		return core.ErrNotOwned
	}
	// Ownership reads do not take the mutation mutex: a slow checkpoint must
	// not serialize Acks behind its network I/O. The server still checks owner,
	// generation, and expiry atomically. Recheck permanent local invalidation
	// after I/O so an old successful response cannot revive this acquisition.
	_, err := validateScript.Exec(ctx, l.client,
		[]string{l.ownersKey, l.expiriesKey, l.generationsKey},
		[]string{l.shardID, l.owner, l.generation}).AsStrSlice()
	if ve, ok := valkey.IsValkeyErr(err); ok && strings.HasPrefix(ve.Error(), "NOTOWNED ") {
		l.markInvalid()
		return core.ErrNotOwned
	}
	if l.invalid.Load() {
		return core.ErrNotOwned
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Binding lets the matching checkpoint backend execute atomic fenced scripts
// through the lease's authoritative client while sharing local invalidation.
type Binding struct{ held *valkeyLease }

// Bind checks the concrete backend, endpoint, database, namespace, and shard.
// Endpoint aliases are deliberately rejected; use the same configuration or
// the store's LeaseManager provider for both dependencies.
func Bind(held core.FencedLease, cfg backend.CheckpointConfig, stream, shard string) (*Binding, error) {
	l, ok := held.(*valkeyLease)
	if !ok || l == nil || l.manager.cfg.Addr != cfg.Addr || l.manager.cfg.DB != cfg.DB ||
		l.manager.cfg.UseCluster != cfg.UseCluster || l.manager.keyPrefix != cfg.LeasePrefix ||
		l.stream != stream || l.shardID != shard {
		return nil, core.ErrLeaseMismatch
	}
	return &Binding{held: l}, nil
}

// Exec runs a script beginning with OwnershipScript. Additional keys must share
// the lease hash tag. Script errors never become successful ownership checks.
func (b *Binding) Exec(ctx context.Context, script *valkey.Lua, keys, args []string) ([]string, error) {
	l := b.held
	if err := l.mu.Lock(ctx); err != nil {
		return nil, err
	}
	defer l.mu.Unlock()
	if l.invalid.Load() {
		return nil, core.ErrNotOwned
	}
	keys = append([]string{l.ownersKey, l.expiriesKey, l.generationsKey}, keys...)
	args = append([]string{l.shardID, l.owner, l.generation}, args...)
	result, err := script.Exec(ctx, l.client, keys, args).AsStrSlice()
	if ve, ok := valkey.IsValkeyErr(err); ok && strings.HasPrefix(ve.Error(), "NOTOWNED ") {
		l.markInvalid()
		return nil, core.ErrNotOwned
	}
	if l.invalid.Load() {
		return nil, core.ErrNotOwned
	}
	return result, err
}
