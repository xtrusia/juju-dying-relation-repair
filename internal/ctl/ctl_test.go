package ctl

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadAgentConfRequiresControllerModel(t *testing.T) {
	path := t.TempDir() + "/agent.conf"
	if err := os.WriteFile(path, []byte("cacert: ca\ncontrollercert: cert\nstatepassword: password\nmodel: model-uuid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	conf, err := LoadAgentConf(path)
	if err != nil {
		t.Fatalf("loading controller agent config: %v", err)
	}
	if conf.ModelUUID != "uuid" {
		t.Fatalf("model UUID = %q, want uuid", conf.ModelUUID)
	}
	if conf.CACert != "ca" {
		t.Fatalf("CA certificate = %q, want ca", conf.CACert)
	}

	if err := os.WriteFile(path, []byte("controllercert: cert\nstatepassword: password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAgentConf(path); err == nil {
		t.Fatal("expected an agent config without a model to be rejected")
	}
}

func mongoTestCertificates(t *testing.T) (string, string, *x509.Certificate) {
	t.Helper()
	var ca *x509.Certificate
	var caKey ed25519.PrivateKey
	var caPEM, serverPEM string
	var leaf *x509.Certificate
	for i := int64(1); i <= 2; i++ {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(i), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			BasicConstraintsValid: true, IsCA: i == 1,
			KeyUsage: x509.KeyUsageDigitalSignature,
			DNSNames: []string{MongoServerName},
		}
		if template.IsCA {
			template.KeyUsage |= x509.KeyUsageCertSign
			ca, caKey = template, private
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, public, caKey)
		if err != nil {
			t.Fatal(err)
		}
		encoded := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		if template.IsCA {
			caPEM = encoded
			continue
		}
		leaf, err = x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		key, err := x509.MarshalPKCS8PrivateKey(private)
		if err != nil {
			t.Fatal(err)
		}
		serverPEM = encoded + string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
	}
	return caPEM, serverPEM, leaf
}

func TestMongoTLSConfig(t *testing.T) {
	ca, server, leaf := mongoTestCertificates(t)
	otherCA, _, _ := mongoTestCertificates(t)
	tests := []struct {
		name, separate, agentCA, server, wantErr string
		caDirectory                              bool
		untrusted                                bool
	}{
		{name: "separate CA", separate: ca, server: server},
		{name: "agent CA", agentCA: ca, server: server},
		{name: "bundled CA", server: server + ca},
		{name: "CA before private key", server: strings.Replace(server, "-----BEGIN PRIVATE KEY-----", ca+"-----BEGIN PRIVATE KEY-----", 1)},
		{name: "separate precedes agent", separate: ca, agentCA: "invalid", server: server},
		{name: "agent precedes bundle", agentCA: ca, server: server + otherCA},
		{name: "invalid separate does not fall back", separate: "invalid", agentCA: ca, server: server + ca, wantErr: "no CA certificate"},
		{name: "unreadable separate does not fall back", caDirectory: true, agentCA: ca, server: server + ca, wantErr: "reading mongo CA"},
		{name: "malformed certificate", separate: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte{1, 2, 3}})), server: server + ca, wantErr: "parsing mongo CA"},
		{name: "invalid agent does not fall back", agentCA: "invalid", server: server + ca, wantErr: "no CA certificate"},
		{name: "leaf is not a CA", server: server, wantErr: "no CA certificate"},
		{name: "unrelated CA cannot verify server", separate: otherCA, server: server + ca, untrusted: true},
		{name: "missing client key", separate: ca, server: ca, wantErr: "loading mongo client certificate"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			certPath := filepath.Join(dir, "server.pem")
			if err := os.WriteFile(certPath, []byte(test.server), 0o600); err != nil {
				t.Fatal(err)
			}
			if test.caDirectory {
				if err := os.Mkdir(filepath.Join(dir, "ca.crt"), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if test.separate != "" {
				if err := os.WriteFile(filepath.Join(dir, "ca.crt"), []byte(test.separate), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := MongoTLSConfig(&AgentConf{CACert: test.agentCA}, certPath)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("got error %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.InsecureSkipVerify || cfg.MinVersion != tls.VersionTLS12 || cfg.ServerName != MongoServerName {
				t.Fatal("Mongo TLS verification settings changed")
			}
			if len(cfg.Certificates) != 1 || !bytes.Equal(cfg.Certificates[0].Certificate[0], leaf.Raw) {
				t.Fatal("client certificate was not loaded")
			}
			_, err = leaf.Verify(x509.VerifyOptions{Roots: cfg.RootCAs, DNSName: cfg.ServerName})
			if (err != nil) != test.untrusted {
				t.Fatalf("server verification error = %v, want untrusted = %v", err, test.untrusted)
			}
			expectedRoots := x509.NewCertPool()
			trustedCA := ca
			if test.untrusted {
				trustedCA = otherCA
			}
			expectedRoots.AppendCertsFromPEM([]byte(trustedCA))
			if !cfg.RootCAs.Equal(expectedRoots) {
				t.Fatal("trust pool must contain only the selected CA, not the server certificate")
			}
		})
	}
}

func TestWriteBackupIsPrivate(t *testing.T) {
	path := t.TempDir() + "/backup.json"
	if err := os.WriteFile(path, []byte("old backup"), 0o644); err != nil {
		t.Fatalf("creating backup: %v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("setting backup permissions: %v", err)
	}
	if err := WriteBackup(path, struct{}{}); err != nil {
		t.Fatalf("writing backup: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stating backup: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("backup permissions = %o, want 600", got)
	}
}

func TestShellQuote(t *testing.T) {
	for in, want := range map[string]string{
		"-machine":                  "-machine",
		"/tmp/tool-backup.json":     "/tmp/tool-backup.json",
		"2m0s":                      "2m0s",
		"":                          "''",
		"my app":                    "'my app'",
		"a;rm -rf /":                "'a;rm -rf /'",
		"it's":                      `'it'\''s'`,
		"$(id)":                     "'$(id)'",
		"aaaaaaaa-bbbb-cccc-dddd-e": "aaaaaaaa-bbbb-cccc-dddd-e",
	} {
		if got := shellQuote(in); got != want {
			t.Errorf("shellQuote(%q) = %s, want %s", in, got, want)
		}
	}
}
