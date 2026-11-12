// Package tlsbootstrap generates a long-lived self-signed certificate
// for Binnacle's query server when one isn't already on disk.
//
// Why this exists: modern browsers (notably Chrome with HTTPS-First
// Mode enabled) refuse to fall back to plain HTTP for any navigation,
// even to a bare LAN IP. Binnacle is LAN-only by design, so a public
// CA isn't an option — but we can still serve TLS by minting our own
// cert at first startup, persisting it in the data volume, and
// reusing it for the next decade.
//
// The cert is *not* a security boundary. It's a transport upgrade
// that satisfies the browser without changing the threat model: the
// LAN ingest path was already plaintext-trusted, and the query path
// has no authentication. Users who hit the UI will see a one-time
// "Not secure" warning, click through, and Chrome remembers the
// exception.
package tlsbootstrap

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// EnsureSelfSigned makes sure both certPath and keyPath exist as a
// usable PEM-encoded ECDSA P-256 cert + key pair. If either is
// missing, it mints a fresh self-signed cert valid for 10 years and
// writes both files atomically. If both already exist, it returns
// without inspecting their content — re-using whatever the operator
// dropped in place.
//
// hostnames is the list of DNS names that should appear in the
// SubjectAltName extension. IPs found in the same list (parsed via
// net.ParseIP) go into the IP-SAN slot instead. Empty entries are
// dropped silently. "localhost" + "127.0.0.1" + "::1" are always
// included so loopback-from-the-container access (the docker
// healthcheck) keeps working without configuration.
func EnsureSelfSigned(certPath, keyPath string, hostnames []string) error {
	if fileExists(certPath) && fileExists(keyPath) {
		return nil
	}

	// Make sure the parent directory exists. The data volume's
	// permissions are inherited from the Dockerfile's chown, so
	// 0o755 directory + 0o600 key file are safe under the same
	// non-root user.
	if err := os.MkdirAll(filepath.Dir(certPath), 0o755); err != nil {
		return fmt.Errorf("mkdir for cert: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o755); err != nil {
		return fmt.Errorf("mkdir for key: %w", err)
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate ECDSA key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("generate serial: %w", err)
	}

	dnsNames, ipAddrs := splitHosts(append([]string{"localhost"}, hostnames...))
	ipAddrs = append(ipAddrs, net.IPv4(127, 0, 0, 1), net.IPv6loopback)
	ipAddrs = dedupIPs(ipAddrs)

	now := time.Now().UTC()
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "binnacle (self-signed)",
			Organization: []string{"Binnacle"},
		},
		NotBefore: now.Add(-1 * time.Hour),
		// 10 years — long enough that the operator never has to
		// think about it; renewing means deleting the files and
		// restarting the process.
		NotAfter:              now.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		DNSNames:              dnsNames,
		IPAddresses:           ipAddrs,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return fmt.Errorf("create cert: %w", err)
	}

	if err := writePEM(certPath, "CERTIFICATE", derBytes, 0o644); err != nil {
		return fmt.Errorf("write cert: %w", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return fmt.Errorf("marshal key: %w", err)
	}
	if err := writePEM(keyPath, "EC PRIVATE KEY", keyDER, 0o600); err != nil {
		return fmt.Errorf("write key: %w", err)
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// splitHosts parses each entry as either a DNS name or an IP address
// (ParseIP is the discriminator). Empty/whitespace entries drop out.
func splitHosts(hosts []string) (dns []string, ips []net.IP) {
	seen := map[string]bool{}
	for _, h := range hosts {
		h = trimSpace(h)
		if h == "" || seen[h] {
			continue
		}
		seen[h] = true
		if ip := net.ParseIP(h); ip != nil {
			ips = append(ips, ip)
		} else {
			dns = append(dns, h)
		}
	}
	return dns, ips
}

func dedupIPs(in []net.IP) []net.IP {
	out := make([]net.IP, 0, len(in))
	seen := map[string]bool{}
	for _, ip := range in {
		key := ip.String()
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, ip)
	}
	return out
}

// trimSpace is the same as strings.TrimSpace but inline to keep the
// import set minimal — this package is intentionally hermetic so it
// can be reused without dragging in helpers.
func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t' || s[0] == '\n' || s[0] == '\r') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t' || s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

// writePEM writes a single PEM block atomically: write to a tmpfile
// in the same directory, fsync, rename. Avoids a half-written cert if
// the process is killed mid-init.
func writePEM(path, blockType string, der []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tls-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if err := pem.Encode(tmp, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Chmod(tmpName, mode); err != nil {
		cleanup()
		return err
	}
	return os.Rename(tmpName, path)
}
