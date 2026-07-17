package backend

import (
	"crypto/tls"
	"fmt"
	"strings"
	"testing"
)

func TestSetAuthRequiresPassword(t *testing.T) {
	var ckpt CheckpointConfig
	if err := SetCheckpointAuth(&ckpt, "user", ""); err == nil {
		t.Fatal("SetCheckpointAuth with empty password: want error, got nil")
	}
	var lease LeaseConfig
	if err := SetLeaseAuth(&lease, "user", ""); err == nil {
		t.Fatal("SetLeaseAuth with empty password: want error, got nil")
	}
}

func TestSetAuthPasswordOnlyAndACLUser(t *testing.T) {
	var ckpt CheckpointConfig
	if err := SetCheckpointAuth(&ckpt, "", "pw"); err != nil {
		t.Fatalf("SetCheckpointAuth password-only: %v", err)
	}
	if ckpt.Username != "" || ckpt.Password != "pw" {
		t.Fatalf("password-only auth = (%q, %q), want (\"\", \"pw\")", ckpt.Username, ckpt.Password)
	}

	var lease LeaseConfig
	if err := SetLeaseAuth(&lease, "acl-user", "pw"); err != nil {
		t.Fatalf("SetLeaseAuth ACL user: %v", err)
	}
	if lease.Username != "acl-user" || lease.Password != "pw" {
		t.Fatalf("ACL auth = (%q, %q), want (\"acl-user\", \"pw\")", lease.Username, lease.Password)
	}
}

func TestSetCredentialsFnRejectsNil(t *testing.T) {
	var ckpt CheckpointConfig
	if err := SetCheckpointCredentialsFn(&ckpt, nil); err == nil {
		t.Fatal("SetCheckpointCredentialsFn(nil): want error, got nil")
	}
	var lease LeaseConfig
	if err := SetLeaseCredentialsFn(&lease, nil); err == nil {
		t.Fatal("SetLeaseCredentialsFn(nil): want error, got nil")
	}
}

func TestSetTLSConfigRejectsNil(t *testing.T) {
	var ckpt CheckpointConfig
	if err := SetCheckpointTLSConfig(&ckpt, nil); err == nil {
		t.Fatal("SetCheckpointTLSConfig(nil): want error, got nil")
	}
	var lease LeaseConfig
	if err := SetLeaseTLSConfig(&lease, nil); err == nil {
		t.Fatal("SetLeaseTLSConfig(nil): want error, got nil")
	}
}

// The stored TLS config must be a private clone: mutating the caller's config
// after the setter returns must not affect the backend's copy, and
// CloneTLSConfig must hand out fresh copies rather than the stored pointer.
func TestSetTLSConfigClonesCallerConfig(t *testing.T) {
	caller := &tls.Config{ServerName: "valkey.internal"}

	var cfg CheckpointConfig
	if err := SetCheckpointTLSConfig(&cfg, caller); err != nil {
		t.Fatalf("SetCheckpointTLSConfig: %v", err)
	}
	if !cfg.UseTLS {
		t.Fatal("SetCheckpointTLSConfig must imply UseTLS")
	}
	if cfg.TLSConfig == caller {
		t.Fatal("stored TLS config is the caller's pointer, want a clone")
	}

	caller.ServerName = "mutated.later"
	if got := cfg.TLSConfig.ServerName; got != "valkey.internal" {
		t.Fatalf("stored clone ServerName = %q after caller mutation, want %q", got, "valkey.internal")
	}

	first, second := cfg.CloneTLSConfig(), cfg.CloneTLSConfig()
	if first == cfg.TLSConfig || first == second {
		t.Fatal("CloneTLSConfig must return fresh copies, not shared pointers")
	}
	if first.ServerName != "valkey.internal" {
		t.Fatalf("clone ServerName = %q, want %q", first.ServerName, "valkey.internal")
	}
}

func TestCloneTLSConfigNilWhenUnset(t *testing.T) {
	var cfg LeaseConfig
	if got := cfg.CloneTLSConfig(); got != nil {
		t.Fatalf("CloneTLSConfig on unset config = %v, want nil", got)
	}
}

func TestFinalizeRejectsStaticAuthPlusProvider(t *testing.T) {
	ckpt := DefaultCheckpointConfig("addr")
	if err := SetCheckpointAuth(&ckpt, "u", "p"); err != nil {
		t.Fatal(err)
	}
	if err := SetCheckpointCredentialsFn(&ckpt, func() (string, string, error) { return "u", "p", nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := FinalizeCheckpointConfig(ckpt, "valkey"); err == nil {
		t.Fatal("FinalizeCheckpointConfig with static auth + provider: want error, got nil")
	}

	lease := DefaultLeaseConfig("addr")
	if err := SetLeaseAuth(&lease, "", "p"); err != nil {
		t.Fatal(err)
	}
	if err := SetLeaseCredentialsFn(&lease, func() (string, string, error) { return "", "p", nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := FinalizeLeaseConfig(lease, "valkey"); err == nil {
		t.Fatal("FinalizeLeaseConfig with static auth + provider: want error, got nil")
	}
}

func TestFinalizeAcceptsEachAuthModeAlone(t *testing.T) {
	ckpt := DefaultCheckpointConfig("addr")
	if err := SetCheckpointAuth(&ckpt, "u", "p"); err != nil {
		t.Fatal(err)
	}
	if _, err := FinalizeCheckpointConfig(ckpt, "valkey"); err != nil {
		t.Fatalf("FinalizeCheckpointConfig static auth: %v", err)
	}

	lease := DefaultLeaseConfig("addr")
	if err := SetLeaseCredentialsFn(&lease, func() (string, string, error) { return "u", "p", nil }); err != nil {
		t.Fatal(err)
	}
	got, err := FinalizeLeaseConfig(lease, "valkey")
	if err != nil {
		t.Fatalf("FinalizeLeaseConfig provider auth: %v", err)
	}
	if got.Credentials == nil {
		t.Fatal("FinalizeLeaseConfig dropped the credentials provider")
	}
}

// Formatting a config through every fmt path (%v, %+v, %s, %#v) must never
// emit the configured secret — the password is redacted and the provider is
// reduced to a presence marker.
func TestConfigFormattingNeverLeaksSecrets(t *testing.T) {
	const secret = "s3cr3t-pw"

	ckpt := DefaultCheckpointConfig("addr")
	if err := SetCheckpointAuth(&ckpt, "user", secret); err != nil {
		t.Fatal(err)
	}
	lease := DefaultLeaseConfig("addr")
	if err := SetLeaseAuth(&lease, "user", secret); err != nil {
		t.Fatal(err)
	}

	for _, verb := range []string{"%v", "%+v", "%s", "%#v"} {
		for name, v := range map[string]any{"CheckpointConfig": ckpt, "LeaseConfig": lease} {
			out := fmt.Sprintf(verb, v)
			if strings.Contains(out, secret) {
				t.Fatalf("%s formatted with %s leaks the password: %s", name, verb, out)
			}
			if !strings.Contains(out, redactedPassword) {
				t.Fatalf("%s formatted with %s does not mark the password as redacted: %s", name, verb, out)
			}
		}
	}
}

// An unset password must not render the redaction marker (which would imply a
// password is configured), and a provider must render as a presence marker.
func TestConfigFormattingPresenceMarkers(t *testing.T) {
	plain := DefaultCheckpointConfig("addr")
	if out := fmt.Sprintf("%v", plain); strings.Contains(out, redactedPassword) {
		t.Fatalf("config without auth renders a redacted password: %s", out)
	}

	withFn := DefaultLeaseConfig("addr")
	if err := SetLeaseCredentialsFn(&withFn, func() (string, string, error) { return "", "p", nil }); err != nil {
		t.Fatal(err)
	}
	if out := fmt.Sprintf("%v", withFn); !strings.Contains(out, "Credentials:<set>") {
		t.Fatalf("config with provider does not mark Credentials as set: %s", out)
	}
}
