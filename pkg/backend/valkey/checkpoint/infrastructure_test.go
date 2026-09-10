//go:build integration

package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	core "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

func dockerTest(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}
func waitValkey(t *testing.T, name string, port int, cluster bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		args := []string{"exec", name, "valkey-cli", "-p", fmt.Sprint(port)}
		want := "PONG"
		if cluster {
			args = append(args, "cluster", "info")
			want = "cluster_state:ok"
		} else {
			args = append(args, "ping")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		cancel()
		if err == nil && strings.Contains(string(out), want) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("Valkey did not become ready; container logs:\n%s", dockerTest(t, "logs", name))
}
func TestRealValkeyClusterFencing(t *testing.T) {
	name := fmt.Sprintf("kcg-slice3-cluster-%d", time.Now().UnixNano())
	script := `set -e
for port in 17000 17001 17002; do
 mkdir -p /data/$port
 valkey-server --port $port --bind 0.0.0.0 --protected-mode no --cluster-enabled yes --cluster-announce-ip 127.0.0.1 --cluster-config-file nodes.conf --dir /data/$port --appendonly yes --appendfsync always --maxmemory-policy noeviction --daemonize yes
done
for port in 17000 17001 17002; do
 until valkey-cli -p $port ping | grep -q PONG; do sleep 0.1; done
done
valkey-cli --cluster create 127.0.0.1:17000 127.0.0.1:17001 127.0.0.1:17002 --cluster-replicas 0 --cluster-yes
tail -f /dev/null`
	dockerTest(t, "run", "-d", "--name", name, "-p", "127.0.0.1:17000:17000", "-p", "127.0.0.1:17001:17001", "-p", "127.0.0.1:17002:17002", "valkey/valkey:8-alpine", "sh", "-c", script)
	t.Cleanup(func() { dockerTest(t, "rm", "-f", "-v", name) })
	waitValkey(t, name, 17000, true)
	s, err := New("127.0.0.1:17000", WithCluster(), WithKeyPrefix("test{prefix}%"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m := managerTest(t, s)
	ctx := context.Background()
	// Exercise different slots, adversarial prefix/identity encoding and the empty identity.
	for _, stream := range []string{"", "group:stream", "{other}:stream", "different"} {
		held, ok, err := m.Acquire(ctx, stream, "shard", "a", time.Minute)
		if err != nil || !ok {
			t.Fatalf("acquire %v %v", ok, err)
		}
		session, err := s.Bind(ctx, stream, "shard", held.(lease.FencedLease))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = session.Initialize(ctx, "10"); err != nil {
			t.Fatal(err)
		}
		if err = session.Save(ctx, "20"); err != nil {
			t.Fatal(err)
		}
		replacement, ok, err := m.Claim(ctx, stream, "shard", "a", "b", time.Minute)
		if err != nil || !ok {
			t.Fatal(err)
		}
		if err = session.Save(ctx, "SHARD_END:20"); !errors.Is(err, lease.ErrNotOwned) {
			t.Fatalf("stale completion %v", err)
		}
		next, err := s.Bind(ctx, stream, "shard", replacement.(lease.FencedLease))
		if err != nil {
			t.Fatal(err)
		}
		if err = next.Save(ctx, "SHARD_END:20"); err != nil {
			t.Fatal(err)
		}
		if err = replacement.Release(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Legacy checkpoint was untagged: rejection must scan beyond the seed node.
	dockerTest(t, "exec", name, "valkey-cli", "-c", "-p", "17002", "set", "old:v2:c3RyZWFt:c2hhcmQ", "10")
	old, err := New("127.0.0.1:17000", WithCluster(), WithKeyPrefix("old"))
	if old != nil {
		old.Close()
	}
	if !errors.Is(err, ErrIncompatibleLayout) {
		t.Fatalf("cluster layout accepted: %v", err)
	}
}

func TestRealValkeyAOFRestart(t *testing.T) {
	name := fmt.Sprintf("kcg-slice3-aof-%d", time.Now().UnixNano())
	dockerTest(t, "run", "-d", "--name", name, "-p", "127.0.0.1:17379:6379", "valkey/valkey:8-alpine", "valkey-server", "--appendonly", "yes", "--appendfsync", "always", "--maxmemory-policy", "noeviction", "--save", "")
	t.Cleanup(func() { dockerTest(t, "rm", "-f", "-v", name) })
	waitValkey(t, name, 6379, false)
	s, err := New("127.0.0.1:17379")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m := managerTest(t, s)
	ctx := context.Background()
	for _, kind := range []string{"initial", "checkpoint", "completed"} {
		held, session := bindTest(t, s, m, kind)
		if _, err = session.Initialize(ctx, "10"); err != nil {
			t.Fatal(err)
		}
		if kind != "initial" {
			v := "20"
			if kind == "completed" {
				v = "SHARD_END:20"
			}
			if err = session.Save(ctx, v); err != nil {
				t.Fatal(err)
			}
		}
		if err = held.Release(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// SIGKILL prevents graceful shutdown from masking missing AOF persistence.
	dockerTest(t, "kill", "--signal", "KILL", name)
	dockerTest(t, "start", name)
	waitValkey(t, name, 6379, false)
	fresh, err := New("127.0.0.1:17379")
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	successors := managerTest(t, fresh)
	for _, kind := range []string{"initial", "checkpoint", "completed"} {
		_, session := bindTest(t, fresh, successors, kind)
		p, err := session.Recovery(ctx)
		want := "10"
		if kind == "checkpoint" {
			want = "20"
		}
		if kind == "completed" {
			want = "SHARD_END:20"
		}
		if err != nil || p.Kind != core.RecoveryKind(kind) || p.Sequence != want {
			t.Fatalf("restart %s: %v %v", kind, p, err)
		}
	}
}
