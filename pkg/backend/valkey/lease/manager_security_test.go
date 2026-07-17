package lease

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/internal/testtls"
)

// tryAcquireRoundtrip exercises the manager end-to-end — acquire, list,
// release — on the given shard, returning the first error so callers can
// either fail immediately or retry across a reconnect window. Retrying
// callers must use a fresh shard ID per attempt: a partially-failed attempt
// can leave its lease held, so a reused shard would report "not acquired"
// until its indexed expiration.
func tryAcquireRoundtrip(mgr *Manager, shardID string) error {
	ctx := context.Background()

	lease, acquired, err := mgr.Acquire(ctx, "grp:stream", shardID, "owner-a", time.Minute)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	if !acquired {
		return errors.New("acquire: not acquired")
	}
	owners, err := mgr.List(ctx, "grp:stream")
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}
	if owners[shardID] != "owner-a" {
		return fmt.Errorf("list = %v, want %s owned by owner-a", owners, shardID)
	}
	if err := lease.Release(ctx); err != nil {
		return fmt.Errorf("release: %w", err)
	}
	return nil
}

// acquireRoundtrip proves the manager is actually usable (not just
// constructed) by acquiring, listing, and releasing a lease.
func acquireRoundtrip(t *testing.T, mgr *Manager) {
	t.Helper()
	if err := tryAcquireRoundtrip(mgr, "shard-1"); err != nil {
		t.Fatal(err)
	}
}

func TestNewManagerPasswordOnlyAuth(t *testing.T) {
	t.Parallel()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(server.Close)
	server.RequireAuth("default-pw")

	if _, err := NewManager(server.Addr()); err == nil {
		t.Fatal("NewManager without credentials against an authenticated server: want error, got nil")
	}

	mgr, err := NewManager(server.Addr(), WithAuth("", "default-pw"))
	if err != nil {
		t.Fatalf("NewManager with password-only auth: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	acquireRoundtrip(t, mgr)
}

func TestNewManagerACLUserAuth(t *testing.T) {
	t.Parallel()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(server.Close)
	server.RequireUserAuth("app", "app-pw")

	mgr, err := NewManager(server.Addr(), WithAuth("app", "app-pw"))
	if err != nil {
		t.Fatalf("NewManager with ACL credentials: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	acquireRoundtrip(t, mgr)

	if _, err := NewManager(server.Addr(), WithAuth("app", "wrong-pw")); err == nil {
		t.Fatal("NewManager with wrong password: want error, got nil")
	}
	if _, err := NewManager(server.Addr(), WithAuth("intruder", "app-pw")); err == nil {
		t.Fatal("NewManager with wrong username: want error, got nil")
	}
}

// rotatingManagerCredentials is a concurrency-safe CredentialsFn source whose
// credentials can be swapped mid-test, standing in for a secret-store-backed
// provider.
type rotatingManagerCredentials struct {
	mu       sync.Mutex
	username string
	password string
	calls    int
}

func (r *rotatingManagerCredentials) rotate(username, password string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.username, r.password = username, password
}

func (r *rotatingManagerCredentials) get() (string, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	return r.username, r.password, nil
}

func (r *rotatingManagerCredentials) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func TestNewManagerCredentialsProvider(t *testing.T) {
	t.Parallel()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(server.Close)
	server.RequireUserAuth("app", "app-pw")

	creds := &rotatingManagerCredentials{username: "app", password: "app-pw"}
	mgr, err := NewManager(server.Addr(), WithCredentialsProvider(creds.get))
	if err != nil {
		t.Fatalf("NewManager with credentials provider: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	acquireRoundtrip(t, mgr)
	if creds.callCount() == 0 {
		t.Fatal("credentials provider was never consulted")
	}

	providerErr := errors.New("secret store unavailable")
	_, err = NewManager(server.Addr(), WithCredentialsProvider(func() (string, string, error) {
		return "", "", providerErr
	}))
	if err == nil {
		t.Fatal("NewManager with failing provider: want error, got nil")
	}
	if !strings.Contains(err.Error(), providerErr.Error()) {
		t.Fatalf("NewManager with failing provider: error %q does not surface the provider cause %q", err, providerErr)
	}
}

// waitForManagerReconnect retries op until it succeeds, failing the test if
// the client has not recovered within the deadline. It exists because a
// client whose connections were dropped observes one or more transport errors
// before its reconnect (with freshly supplied credentials) completes.
func waitForManagerReconnect(t *testing.T, op func() error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		if err = op(); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("client did not recover before deadline; last error: %v", err)
}

// Rotation: the provider is consulted on every connection attempt, so when
// the server rotates its password and drops all connections, the SAME live
// manager reconnects with the rotated credentials — no rebuild — while a
// manager holding static credentials captured before the rotation stays
// locked out.
func TestNewManagerCredentialsProviderRotation(t *testing.T) {
	t.Parallel()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(server.Close)
	server.RequireUserAuth("app", "pw-v1")

	creds := &rotatingManagerCredentials{username: "app", password: "pw-v1"}
	mgr, err := NewManager(server.Addr(), WithCredentialsProvider(creds.get))
	if err != nil {
		t.Fatalf("NewManager with credentials provider: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	acquireRoundtrip(t, mgr)

	staticMgr, err := NewManager(server.Addr(), WithAuth("app", "pw-v1"))
	if err != nil {
		t.Fatalf("NewManager with static credentials: %v", err)
	}
	t.Cleanup(func() { _ = staticMgr.Close() })

	callsBefore := creds.callCount()

	// Rotate: restart the server on the same address accepting only the new
	// password (dropping every live connection) and update the provider's
	// source of truth.
	server.Close()
	server.RequireUserAuth("app", "pw-v2")
	creds.rotate("app", "pw-v2")
	if err := server.Restart(); err != nil {
		t.Fatalf("miniredis restart: %v", err)
	}

	// The SAME manager must reconnect with the rotated credentials. The full
	// roundtrip is the retried probe: the manager holds more than one
	// connection (List fans out via Nodes()), and every one must recover.
	// Each attempt probes a fresh shard so a partially-failed attempt cannot
	// leave the next one locked out (see tryAcquireRoundtrip).
	ctx := context.Background()
	attempt := 0
	waitForManagerReconnect(t, func() error {
		attempt++
		return tryAcquireRoundtrip(mgr, fmt.Sprintf("shard-reconnect-%d", attempt))
	})
	if got := creds.callCount(); got <= callsBefore {
		t.Fatalf("provider consulted %d time(s) after rotation, want more than the %d before it", got, callsBefore)
	}

	// The static-credential manager captured pw-v1 at option time and must
	// stay locked out after its reconnect attempt.
	if _, err := staticMgr.List(ctx, "grp:stream"); err == nil {
		t.Fatal("static-credential manager survived the rotation: want error, got nil")
	}
}

func TestNewManagerTLSCustomCA(t *testing.T) {
	t.Parallel()

	certs := testtls.New(t, nil, []net.IP{net.ParseIP("127.0.0.1")})
	server, err := miniredis.RunTLS(certs.ServerConfig)
	if err != nil {
		t.Fatalf("miniredis TLS start: %v", err)
	}
	t.Cleanup(server.Close)

	mgr, err := NewManager(server.Addr(), WithTLSConfig(&tls.Config{RootCAs: certs.CAPool}))
	if err != nil {
		t.Fatalf("NewManager with custom CA: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	acquireRoundtrip(t, mgr)

	// Default TLS (system roots) must reject the test CA.
	if _, err := NewManager(server.Addr(), WithTLS()); err == nil {
		t.Fatal("NewManager with system roots against a private CA: want error, got nil")
	}
}

func TestNewManagerTLSHostnameVerification(t *testing.T) {
	t.Parallel()

	// The certificate is valid only for the DNS name, not for 127.0.0.1.
	certs := testtls.New(t, []string{"valkey.test"}, nil)
	server, err := miniredis.RunTLS(certs.ServerConfig)
	if err != nil {
		t.Fatalf("miniredis TLS start: %v", err)
	}
	t.Cleanup(server.Close)

	if _, err := NewManager(server.Addr(), WithTLSConfig(&tls.Config{RootCAs: certs.CAPool})); err == nil {
		t.Fatal("NewManager with hostname-mismatched certificate: want error, got nil")
	}

	mgr, err := NewManager(server.Addr(), WithTLSConfig(&tls.Config{
		RootCAs:    certs.CAPool,
		ServerName: "valkey.test",
	}))
	if err != nil {
		t.Fatalf("NewManager with matching ServerName: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	acquireRoundtrip(t, mgr)
}

// The caller's TLS config is cloned at the option boundary: gutting it after
// construction must not break the manager's later connections.
func TestNewManagerTLSConfigClonedFromCaller(t *testing.T) {
	t.Parallel()

	certs := testtls.New(t, nil, []net.IP{net.ParseIP("127.0.0.1")})
	server, err := miniredis.RunTLS(certs.ServerConfig)
	if err != nil {
		t.Fatalf("miniredis TLS start: %v", err)
	}
	t.Cleanup(server.Close)

	caller := &tls.Config{RootCAs: certs.CAPool}
	mgr, err := NewManager(server.Addr(), WithTLSConfig(caller))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	caller.RootCAs = nil
	caller.ServerName = "mutated.invalid"
	acquireRoundtrip(t, mgr)
}
