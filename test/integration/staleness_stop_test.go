//go:build integration

// Staleness-chain scenarios: a real Valkey outage mid-run must first be
// survivable (failures recorded, run still alive) and then stop the run with
// the documented staleness error — ErrHeartbeatStale before peers may treat
// the worker as dead, ErrShardSyncStale once the shard map can no longer be
// trusted.
//
// The outage is a severed TCP path, not a paused shared container: the
// consumer under test connects to Valkey through a per-test local proxy whose
// teardown closes every live connection and refuses new ones — exactly what a
// stopped container looks like to the client — while parallel tests and the
// test's own direct assertions keep their healthy connections.
package integration

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	valkeycheckpoint "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

// tcpProxy forwards TCP connections to a target address until stop severs
// them all. stop is idempotent and registered as a cleanup.
type tcpProxy struct {
	listener net.Listener
	addr     string

	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	stopped bool
}

func startTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("start tcp proxy listener: %v", err)
	}
	p := &tcpProxy{
		listener: listener,
		addr:     listener.Addr().String(),
		conns:    make(map[net.Conn]struct{}),
	}
	go p.acceptLoop(target)
	t.Cleanup(p.stop)
	return p
}

func (p *tcpProxy) acceptLoop(target string) {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", target)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.track(client)
		p.track(upstream)
		go proxyPipe(client, upstream)
		go proxyPipe(upstream, client)
	}
}

func proxyPipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
}

func (p *tcpProxy) track(conn net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		_ = conn.Close()
		return
	}
	p.conns[conn] = struct{}{}
}

// stop severs the proxied path: no new connections are accepted and every
// live connection is closed, so the client sees the same thing as a stopped
// Valkey container — prompt resets now, connection refused from here on.
func (p *tcpProxy) stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	conns := make([]net.Conn, 0, len(p.conns))
	for conn := range p.conns {
		conns = append(conns, conn)
	}
	p.mu.Unlock()

	_ = p.listener.Close()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

// newStoreAt mirrors newStore but connects to an explicit address (the proxy).
func newStoreAt(t *testing.T, addr, keyPrefix string) *valkeycheckpoint.Store {
	t.Helper()
	store, err := valkeycheckpoint.New(addr, valkeycheckpoint.WithKeyPrefix(keyPrefix))
	if err != nil {
		t.Fatalf("connect valkey at %s: %v", addr, err)
	}
	return store
}

// blockLeaseWithLiveOwner takes the shard's lease as a synthetic peer that
// also heartbeats, so the consumer under test never owns a lease: it sees the
// shard held by a live worker and stays a heartbeating, syncing bystander.
// This pins the staleness chains to their own loops — with no owned lease
// there is no renew loop whose independent TTL stop could race the heartbeat
// staleness stop and flake the test (both bounds derive from the same
// heartbeat interval/TTL knobs, and their worst-case phase alignment is a
// dead heat by construction).
func blockLeaseWithLiveOwner(ctx context.Context, t *testing.T, manager lease.Manager, stream, shardID string) {
	t.Helper()
	const blockerTTL = 10 * time.Minute
	identity := integrationCoordinationIdentity(stream)
	blockerLease, acquired, err := manager.Acquire(ctx, identity, shardID, "blocker", blockerTTL)
	if err != nil || !acquired {
		t.Fatalf("acquire blocker lease for %s: acquired=%v err=%v", shardID, acquired, err)
	}
	t.Cleanup(func() {
		_ = blockerLease.Release(context.Background())
	})
	if err := manager.Heartbeat(ctx, identity, "blocker", blockerTTL); err != nil {
		t.Fatalf("heartbeat blocker worker key: %v", err)
	}
}

// waitForStaleness polls cond (a Health-based observation) until it holds,
// failing the test on timeout.
func waitForStaleness(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// IT-29: survivable heartbeat failures -> ErrHeartbeatStale stop before peers
// may treat the worker as dead.
func TestValkeyOutageStopsRunWithHeartbeatStaleWhileWorkerKeyLive(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := newKinesisClient()
	stream := uniqueName("hb-stale")
	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)
	shardID := listShardIDs(ctx, t, client, stream)[0]

	prefix := uniqueName("hb-stale-keys")
	directStore := newStore(t, prefix)
	directManager := newLeaseManager(t, directStore)
	blockLeaseWithLiveOwner(ctx, t, directManager, stream, shardID)

	proxy := startTCPProxy(t, valkeyAddr())
	proxiedStore := newStoreAt(t, proxy.addr, prefix)

	// heartbeat interval 2s / TTL 8s: the validity stop fires at
	// lastSuccess+6s, a full two-interval margin before the worker key written
	// by that success expires at lastSuccess+8s — enough room to observe the
	// key still live after Start returns, even on a slow machine. Sync and
	// rebalance cadences are pushed past the test window so their own
	// outage failures cannot interleave.
	cfg := consumer.Config{
		StreamName:    stream,
		ConsumerGroup: integrationConsumerGroup,
		StartPosition: consumer.StartTrimHorizon,
	}
	cons, err := consumer.New(cfg, client, proxiedStore,
		func(context.Context, consumer.Record) error { return nil },
		consumer.WithPolling(200*time.Millisecond, time.Minute),
		consumer.WithHeartbeat(2*time.Second, 8*time.Second),
		consumer.WithRebalance(time.Minute, 0, 500*time.Millisecond, 2),
	)
	if err != nil {
		t.Fatalf("create consumer for %s: %v", stream, err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	errCh := make(chan error, 1)
	go func() { errCh <- cons.Start(runCtx) }()

	// Mid-lease: the blocker holds the shard's lease and both workers are
	// visible before the outage begins.
	waitForLeaseWorkers(t, directManager, stream, 2, 30*time.Second)
	// The heartbeat loop writes its worker key before Start's initial
	// acquisition pass finishes, and errors on that initial pass are
	// deliberately fatal — so also wait for the pass to complete (it anchors
	// the shard-sync staleness clock) before severing the connection, or the
	// outage races startup instead of hitting a running consumer.
	waitForStaleness(t, 30*time.Second, "initial shard discovery to complete", func() bool {
		return !cons.Health().ShardSync.LastSuccess.IsZero()
	})

	proxy.stop()

	// Survivable first: heartbeat failures are recorded while the run stays
	// up (the first failure lands at least two intervals before the stop).
	waitForStaleness(t, 15*time.Second, "a recorded heartbeat failure", func() bool {
		return cons.Health().Heartbeat.ConsecutiveFailures >= 1
	})
	select {
	case err := <-errCh:
		t.Fatalf("consumer stopped on the first heartbeat failures instead of surviving them: %v", err)
	default:
	}

	// Then the documented stop, inside the safety margin.
	select {
	case err := <-errCh:
		if !errors.Is(err, consumer.ErrHeartbeatStale) {
			t.Fatalf("Start() error = %v, want wraps ErrHeartbeatStale", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("consumer did not stop with ErrHeartbeatStale after the Valkey outage")
	}

	// "Before peers claim": the worker key written by the last successful
	// heartbeat is still live at stop time, so no peer may treat this worker
	// as dead yet — the run stopped inside the margin, not after it.
	workers, err := directManager.Workers(ctx, integrationCoordinationIdentity(stream))
	if err != nil {
		t.Fatalf("list workers after stop: %v", err)
	}
	if len(workers) != 2 {
		t.Fatalf("live workers after ErrHeartbeatStale stop = %v, want 2 (blocker + the stopped consumer's still-unexpired key)", workers)
	}
}

// IT-30: survivable shard-sync failures -> ErrShardSyncStale stop once the
// staleness bound lapses.
func TestValkeyOutageStopsRunWithShardSyncStale(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := newKinesisClient()
	stream := uniqueName("sync-stale")
	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)
	shardID := listShardIDs(ctx, t, client, stream)[0]

	prefix := uniqueName("sync-stale-keys")
	directStore := newStore(t, prefix)
	directManager := newLeaseManager(t, directStore)
	blockLeaseWithLiveOwner(ctx, t, directManager, stream, shardID)

	proxy := startTCPProxy(t, valkeyAddr())
	proxiedStore := newStoreAt(t, proxy.addr, prefix)

	// Sync every second with a 3s staleness bound: each pass reads the
	// blocked shard's checkpoint through the severed proxy and fails, so the
	// bound trips after a few survivable failures. The heartbeat TTL is
	// enormous so its own staleness stop stays far outside the test window,
	// and rebalance passes are pushed out entirely.
	cfg := consumer.Config{
		StreamName:    stream,
		ConsumerGroup: integrationConsumerGroup,
		StartPosition: consumer.StartTrimHorizon,
	}
	cons, err := consumer.New(cfg, client, proxiedStore,
		func(context.Context, consumer.Record) error { return nil },
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithShardSyncMaxStaleness(3*time.Second),
		consumer.WithHeartbeat(2*time.Second, 10*time.Minute),
		consumer.WithRebalance(time.Minute, 0, 500*time.Millisecond, 2),
		consumer.WithRetry(2, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("create consumer for %s: %v", stream, err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	errCh := make(chan error, 1)
	go func() { errCh <- cons.Start(runCtx) }()

	waitForLeaseWorkers(t, directManager, stream, 2, 30*time.Second)
	// See the heartbeat test: the outage must not race Start's fatal initial
	// acquisition pass; a non-zero sync LastSuccess means it completed.
	waitForStaleness(t, 30*time.Second, "initial shard discovery to complete", func() bool {
		return !cons.Health().ShardSync.LastSuccess.IsZero()
	})

	proxy.stop()

	// Survivable first: failed sync passes are recorded while the run stays
	// up until the staleness bound lapses.
	waitForStaleness(t, 15*time.Second, "a recorded shard-sync failure", func() bool {
		return cons.Health().ShardSync.ConsecutiveFailures >= 1
	})
	select {
	case err := <-errCh:
		t.Fatalf("consumer stopped on the first shard-sync failures instead of surviving them: %v", err)
	default:
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, consumer.ErrShardSyncStale) {
			t.Fatalf("Start() error = %v, want wraps ErrShardSyncStale", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("consumer did not stop with ErrShardSyncStale after the Valkey outage")
	}
}
