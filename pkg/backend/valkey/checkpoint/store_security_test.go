package checkpoint

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/internal/testtls"
)

// roundtrip proves the store is actually usable (not just constructed) by
// saving and reading back a checkpoint.
func roundtrip(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	if err := store.Save(ctx, "grp:stream", "shard-1", "49"); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := store.Get(ctx, "grp:stream", "shard-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "49" {
		t.Fatalf("Get = %q, want %q", got, "49")
	}
}

func TestNewPasswordOnlyAuth(t *testing.T) {
	t.Parallel()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(server.Close)
	server.RequireAuth("default-pw")

	if _, err := New(server.Addr()); err == nil {
		t.Fatal("New without credentials against an authenticated server: want error, got nil")
	}

	store, err := New(server.Addr(), WithAuth("", "default-pw"))
	if err != nil {
		t.Fatalf("New with password-only auth: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	roundtrip(t, store)
}

func TestNewACLUserAuth(t *testing.T) {
	t.Parallel()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(server.Close)
	server.RequireUserAuth("app", "app-pw")

	store, err := New(server.Addr(), WithAuth("app", "app-pw"))
	if err != nil {
		t.Fatalf("New with ACL credentials: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	roundtrip(t, store)

	if _, err := New(server.Addr(), WithAuth("app", "wrong-pw")); err == nil {
		t.Fatal("New with wrong password: want error, got nil")
	}
	if _, err := New(server.Addr(), WithAuth("intruder", "app-pw")); err == nil {
		t.Fatal("New with wrong username: want error, got nil")
	}
}

// rotatingCredentials is a concurrency-safe CredentialsFn source whose
// credentials can be swapped mid-test, standing in for a secret-store-backed
// provider.
type rotatingCredentials struct {
	mu       sync.Mutex
	username string
	password string
	calls    int
}

func (r *rotatingCredentials) rotate(username, password string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.username, r.password = username, password
}

func (r *rotatingCredentials) get() (string, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	return r.username, r.password, nil
}

func (r *rotatingCredentials) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func TestNewCredentialsProvider(t *testing.T) {
	t.Parallel()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(server.Close)
	server.RequireUserAuth("app", "app-pw")

	creds := &rotatingCredentials{username: "app", password: "app-pw"}
	store, err := New(server.Addr(), WithCredentialsProvider(creds.get))
	if err != nil {
		t.Fatalf("New with credentials provider: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	roundtrip(t, store)
	if creds.callCount() == 0 {
		t.Fatal("credentials provider was never consulted")
	}

	providerErr := errors.New("secret store unavailable")
	_, err = New(server.Addr(), WithCredentialsProvider(func() (string, string, error) {
		return "", "", providerErr
	}))
	if err == nil {
		t.Fatal("New with failing provider: want error, got nil")
	}
	if !strings.Contains(err.Error(), providerErr.Error()) {
		t.Fatalf("New with failing provider: error %q does not surface the provider cause %q", err, providerErr)
	}
}

// waitForReconnect retries op until it succeeds, failing the test if the
// client has not recovered within the deadline. It exists because a client
// whose connections were dropped observes one or more transport errors before
// its reconnect (with freshly supplied credentials) completes.
func waitForReconnect(t *testing.T, op func() error) {
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
// store reconnects with the rotated credentials — no rebuild — while a store
// holding static credentials captured before the rotation stays locked out.
func TestCredentialsProviderRotation(t *testing.T) {
	t.Parallel()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(server.Close)
	server.RequireUserAuth("app", "pw-v1")

	creds := &rotatingCredentials{username: "app", password: "pw-v1"}
	store, err := New(server.Addr(), WithCredentialsProvider(creds.get))
	if err != nil {
		t.Fatalf("New with credentials provider: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	roundtrip(t, store)

	staticStore, err := New(server.Addr(), WithAuth("app", "pw-v1"))
	if err != nil {
		t.Fatalf("New with static credentials: %v", err)
	}
	t.Cleanup(func() { _ = staticStore.Close() })

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

	// The SAME store must reconnect with the rotated credentials. (Checkpoint
	// saves are advance-only, so the post-rotation proof advances to "50"
	// rather than reusing the "49" roundtrip.)
	ctx := context.Background()
	waitForReconnect(t, func() error { return store.Save(ctx, "grp:stream", "shard-1", "50") })
	got, err := store.Get(ctx, "grp:stream", "shard-1")
	if err != nil {
		t.Fatalf("Get after reconnect: %v", err)
	}
	if got != "50" {
		t.Fatalf("Get after reconnect = %q, want %q", got, "50")
	}
	if got := creds.callCount(); got <= callsBefore {
		t.Fatalf("provider consulted %d time(s) after rotation, want more than the %d before it", got, callsBefore)
	}

	// The static-credential store captured pw-v1 at option time and must stay
	// locked out after its reconnect attempt.
	if _, err := staticStore.Get(ctx, "grp:stream", "shard-1"); err == nil {
		t.Fatal("static-credential store survived the rotation: want error, got nil")
	}
}

func TestNewTLSCustomCA(t *testing.T) {
	t.Parallel()

	certs := testtls.New(t, nil, []net.IP{net.ParseIP("127.0.0.1")})
	server, err := miniredis.RunTLS(certs.ServerConfig)
	if err != nil {
		t.Fatalf("miniredis TLS start: %v", err)
	}
	t.Cleanup(server.Close)

	store, err := New(server.Addr(), WithTLSConfig(&tls.Config{RootCAs: certs.CAPool}))
	if err != nil {
		t.Fatalf("New with custom CA: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	roundtrip(t, store)

	// Default TLS (system roots) must reject the test CA.
	if _, err := New(server.Addr(), WithTLS()); err == nil {
		t.Fatal("New with system roots against a private CA: want error, got nil")
	}
}

func TestNewTLSHostnameVerification(t *testing.T) {
	t.Parallel()

	// The certificate is valid only for the DNS name, not for 127.0.0.1.
	certs := testtls.New(t, []string{"valkey.test"}, nil)
	server, err := miniredis.RunTLS(certs.ServerConfig)
	if err != nil {
		t.Fatalf("miniredis TLS start: %v", err)
	}
	t.Cleanup(server.Close)

	if _, err := New(server.Addr(), WithTLSConfig(&tls.Config{RootCAs: certs.CAPool})); err == nil {
		t.Fatal("New with hostname-mismatched certificate: want error, got nil")
	}

	store, err := New(server.Addr(), WithTLSConfig(&tls.Config{
		RootCAs:    certs.CAPool,
		ServerName: "valkey.test",
	}))
	if err != nil {
		t.Fatalf("New with matching ServerName: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	roundtrip(t, store)
}

// The caller's TLS config is cloned at the option boundary: gutting it after
// construction must not break the store's later connections.
func TestNewTLSConfigClonedFromCaller(t *testing.T) {
	t.Parallel()

	certs := testtls.New(t, nil, []net.IP{net.ParseIP("127.0.0.1")})
	server, err := miniredis.RunTLS(certs.ServerConfig)
	if err != nil {
		t.Fatalf("miniredis TLS start: %v", err)
	}
	t.Cleanup(server.Close)

	caller := &tls.Config{RootCAs: certs.CAPool}
	store, err := New(server.Addr(), WithTLSConfig(caller))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	caller.RootCAs = nil
	caller.ServerName = "mutated.invalid"
	roundtrip(t, store)
}

// A secured store must hand its security settings to the auto-created lease
// manager: against a TLS + ACL server, the derived manager can acquire and
// release leases.
func TestLeaseManagerPropagatesSecurity(t *testing.T) {
	t.Parallel()

	certs := testtls.New(t, nil, []net.IP{net.ParseIP("127.0.0.1")})
	server, err := miniredis.RunTLS(certs.ServerConfig)
	if err != nil {
		t.Fatalf("miniredis TLS start: %v", err)
	}
	t.Cleanup(server.Close)
	server.RequireUserAuth("app", "app-pw")

	tlsConfig := &tls.Config{RootCAs: certs.CAPool}

	for name, opts := range map[string][]Option{
		"static auth": {WithAuth("app", "app-pw"), WithTLSConfig(tlsConfig)},
		"provider auth": {
			WithCredentialsProvider(func() (string, string, error) { return "app", "app-pw", nil }),
			WithTLSConfig(tlsConfig),
		},
	} {
		t.Run(name, func(t *testing.T) {
			store, err := New(server.Addr(), opts...)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = store.Close() })

			manager, err := store.LeaseManager()
			if err != nil {
				t.Fatalf("LeaseManager: %v", err)
			}
			t.Cleanup(func() {
				if closer, ok := manager.(io.Closer); ok {
					_ = closer.Close()
				}
			})

			ctx := context.Background()
			lease, acquired, err := manager.Acquire(ctx, "grp:stream", "shard-1", "owner-a", time.Minute)
			if err != nil {
				t.Fatalf("Acquire through derived manager: %v", err)
			}
			if !acquired {
				t.Fatal("Acquire through derived manager: not acquired")
			}
			if err := lease.Release(ctx); err != nil {
				t.Fatalf("Release: %v", err)
			}
		})
	}
}
