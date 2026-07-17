// Package testtls generates throwaway TLS material for backend tests: a
// self-signed CA and a server certificate signed by it, with caller-chosen
// SANs. It exists so the checkpoint and lease packages can prove custom-CA
// and hostname-verification behavior against a real TLS listener (miniredis
// RunTLS) without shipping fixture files.
package testtls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"
)

// ServerCerts is the generated material for one TLS server.
type ServerCerts struct {
	// CAPool contains only the generated CA; clients trust the server by
	// putting this in tls.Config.RootCAs.
	CAPool *x509.CertPool
	// ServerConfig carries the CA-signed leaf and is passed to the server
	// (for example miniredis.RunTLS).
	ServerConfig *tls.Config
}

// New generates a CA and a server certificate signed by it, valid for the
// given DNS names and IPs (either may be empty to leave that SAN class out,
// which is how hostname-mismatch cases are built).
func New(t *testing.T, dnsNames []string, ips []net.IP) ServerCerts {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kinesis-consumer-go test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "kinesis-consumer-go test server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     dnsNames,
		IPAddresses:  ips,
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server certificate: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	return ServerCerts{
		CAPool: pool,
		ServerConfig: &tls.Config{
			Certificates: []tls.Certificate{{
				Certificate: [][]byte{leafDER},
				PrivateKey:  leafKey,
			}},
		},
	}
}
