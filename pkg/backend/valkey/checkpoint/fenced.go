package checkpoint

import (
	"context"
	"fmt"
	"strings"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/internal/backend"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/internal/ctxlock"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/internal/layout"
	valkeylease "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/lease"
	core "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	valkey "github.com/valkey-io/valkey-go"
)

var _ core.FencedStore = (*Store)(nil)

// ErrIncompatibleLayout reports a namespace containing an older layout.
var ErrIncompatibleLayout = layout.ErrIncompatible

// Bind binds a matching Valkey lease and verifies its recovery state.
func (s *Store) Bind(ctx context.Context, stream, shard string, held lease.FencedLease) (core.Session, error) {
	binding, err := valkeylease.Bind(held, s.cfg, stream, shard)
	if err != nil {
		return nil, err
	}
	session := &session{binding: binding, keys: []string{
		backend.RecoveryRegistryKey(s.keyPrefix, stream), s.key(stream, shard), s.key(stream, shard) + ":initial",
	}}
	if _, err := session.Recovery(ctx); err != nil {
		return nil, err
	}
	return session, nil
}

type session struct {
	mu       ctxlock.Mutex
	binding  *valkeylease.Binding
	keys     []string
	observed core.RecoveryPosition
	invalid  bool
}

func (s *session) Invalidate() {
	_ = s.mu.Lock(context.Background())
	defer s.mu.Unlock()
	s.invalid = true
}
func (s *session) Recovery(ctx context.Context) (core.RecoveryPosition, error) {
	return s.run(ctx, "read", "")
}
func (s *session) Initialize(ctx context.Context, sequence string) (core.RecoveryPosition, error) {
	return s.run(ctx, "initial", sequence)
}
func (s *session) Save(ctx context.Context, sequence string) error {
	_, err := s.run(ctx, "save", sequence)
	return err
}
func (s *session) run(ctx context.Context, op, sequence string) (core.RecoveryPosition, error) {
	if err := s.mu.Lock(ctx); err != nil {
		return core.RecoveryPosition{}, err
	}
	defer s.mu.Unlock()
	if s.invalid {
		return core.RecoveryPosition{}, lease.ErrNotOwned
	}
	result, err := s.binding.Exec(ctx, recoveryScript, s.keys, []string{op, sequence, string(s.observed.Kind), s.observed.Sequence})
	if err != nil {
		if ve, ok := valkey.IsValkeyErr(err); ok && strings.HasPrefix(ve.Error(), "RECOVERY ") {
			return core.RecoveryPosition{}, fmt.Errorf("%w: %s", core.ErrRecoveryState, ve.Error())
		}
		return core.RecoveryPosition{}, err
	}
	if len(result) != 2 {
		return core.RecoveryPosition{}, fmt.Errorf("%w: invalid script response", core.ErrRecoveryState)
	}
	s.observed = core.RecoveryPosition{Kind: core.RecoveryKind(result[0]), Sequence: result[1]}
	return s.observed, nil
}

// KEYS 1..3 and ARGV 1..3 are ownership; KEYS 4..6 are registry,
// checkpoint and initial value. ARGV 4..7 are operation, input and last observation.
// All validation precedes mutation: Lua runtime errors do not roll back writes.
var recoveryScript = valkey.NewLuaScript(valkeylease.OwnershipScript + `
local function seq(v)
 return v and (v == "0" or string.match(v, "^[1-9]%d*$") ~= nil)
end
local function completed(v)
 return v and (v == "SHARD_END" or (string.sub(v,1,10) == "SHARD_END:" and seq(string.sub(v,11))))
end
local function less(a,b)
 return #a < #b or (#a == #b and a < b)
end
local types = {"hash","string","string"}
for i=4,6 do
 local t = redis.call("type",KEYS[i]).ok
 if t ~= "none" and t ~= types[i-3] then return redis.error_reply("RECOVERY wrong metadata type") end
 if redis.call("pttl",KEYS[i]) >= 0 then return redis.error_reply("RECOVERY metadata has TTL") end
end
local kind = redis.call("hget",KEYS[4],ARGV[1]) or ""
local saved = redis.call("get",KEYS[5])
local initial = redis.call("get",KEYS[6])
local value = ""
local valid = false
if kind == "" then valid = not saved and not initial and redis.call("hexists",KEYS[4],ARGV[1]) == 0
elseif kind == "initial" then value = initial; valid = seq(initial) and not saved
elseif kind == "checkpoint" then value = saved; valid = seq(saved) and not initial
elseif kind == "completed" then value = saved; valid = completed(saved) and not initial end
if not valid then return redis.error_reply("RECOVERY inconsistent registry and value") end
local prev, old = ARGV[6], ARGV[7]
if (prev == "initial" and (kind == "" or (kind == "initial" and value ~= old) or (kind == "checkpoint" and less(value,old)))) or
 (prev == "checkpoint" and (kind == "" or kind == "initial" or (kind == "checkpoint" and less(value,old)))) or
 (prev == "completed" and (kind ~= prev or value ~= old)) then
 return redis.error_reply("RECOVERY observed progress regression")
end
local op, next = ARGV[4],ARGV[5]
if op == "initial" and kind == "" then
 if not seq(next) then return redis.error_reply("RECOVERY invalid initial sequence") end
 redis.call("set",KEYS[6],next)
 redis.call("hset",KEYS[4],ARGV[1],"initial")
 return {"initial",next}
elseif op == "save" then
 local nextkind = "checkpoint"
 if completed(next) then nextkind = "completed"
 elseif not seq(next) then return redis.error_reply("RECOVERY invalid checkpoint") end
 if kind == "completed" then return {kind,value} end
 if kind == "initial" and nextkind == "checkpoint" and less(next,value) then
  return redis.error_reply("RECOVERY checkpoint precedes initial position")
 end
 if kind == "checkpoint" and nextkind == "checkpoint" and not less(value,next) then return {kind,value} end
 redis.call("set",KEYS[5],next)
 redis.call("hset",KEYS[4],ARGV[1],nextkind)
 redis.call("del",KEYS[6])
 return {nextkind,next}
end
return {kind,value}
`)
