package backend

import (
	"crypto/tls"
	"errors"
)

// CredentialsFn supplies AUTH credentials dynamically. Concrete backends
// invoke it on each connection attempt (initial dial and every reconnect), so
// rotated credentials are picked up without rebuilding the store or manager.
// An empty username authenticates the default user. It must be safe for
// concurrent use.
type CredentialsFn func() (username, password string, err error)

// redactedPassword replaces a configured password in formatted output so a
// config value can never leak a secret through logging.
const redactedPassword = "[redacted]"

// authConfig is the connection-security portion shared by CheckpointConfig
// and LeaseConfig. Username/Password authenticate statically; Credentials
// supplies them dynamically instead (setting both is rejected at finalize).
// TLSConfig, when non-nil, is a private clone of the caller's config and
// implies TLS.
type authConfig struct {
	Username    string
	Password    string
	Credentials CredentialsFn
	TLSConfig   *tls.Config
}

// setAuth records static credentials. The password is required — Redis-family
// AUTH always takes one — while an empty username means the default user
// (password-only deployments).
func (a *authConfig) setAuth(username, password string) error {
	if password == "" {
		return errors.New("auth password cannot be empty")
	}
	a.Username = username
	a.Password = password
	return nil
}

// setCredentialsFn records a dynamic credential provider, rejecting nil.
func (a *authConfig) setCredentialsFn(fn CredentialsFn) error {
	if fn == nil {
		return errors.New("credentials provider cannot be nil")
	}
	a.Credentials = fn
	return nil
}

// setTLSConfig stores a clone of the caller's TLS config, rejecting nil. The
// clone means later caller mutation cannot affect the backend; callers who
// want plain default TLS use the boolean toggle instead.
func (a *authConfig) setTLSConfig(cfg *tls.Config) error {
	if cfg == nil {
		return errors.New("tls config cannot be nil")
	}
	a.TLSConfig = cfg.Clone()
	return nil
}

// validate rejects contradictory authentication settings; it is called by the
// Finalize functions.
func (a *authConfig) validate(backendName string) error {
	if a.Credentials != nil && (a.Username != "" || a.Password != "") {
		return errors.New(backendName + " static auth credentials and a credentials provider are mutually exclusive")
	}
	return nil
}

// CloneTLSConfig returns a private copy of the stored TLS config (nil when
// unset) so no two clients ever share a mutable tls.Config.
func (a *authConfig) CloneTLSConfig() *tls.Config {
	return a.TLSConfig.Clone()
}

// redactedCredentials renders the provider as a presence marker for
// formatted output.
func (a *authConfig) redactedCredentials() string {
	if a.Credentials == nil {
		return "<nil>"
	}
	return "<set>"
}

// redactedStaticPassword renders the static password as a presence-preserving
// marker for formatted output.
func (a *authConfig) redactedStaticPassword() string {
	if a.Password == "" {
		return ""
	}
	return redactedPassword
}
