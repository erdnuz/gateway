//go:build integration

package integration

import (
	"crypto/rand"
	"crypto/rsa"
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

type mtlsFiles struct {
	HubCertPath  string
	HubKeyPath   string
	EdgeCertPath string
	EdgeKeyPath  string
	CAPath       string
}

func generateMTLSFiles(dir, hubServerName string) (*mtlsFiles, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gate-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, err
	}

	hubPair, err := createSignedLeaf(caCert, caKey, "hub-server", []string{hubServerName, "localhost"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	if err != nil {
		return nil, fmt.Errorf("create hub cert: %w", err)
	}
	edgePair, err := createSignedLeaf(caCert, caKey, "edge-client", nil, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	if err != nil {
		return nil, fmt.Errorf("create edge cert: %w", err)
	}

	files := &mtlsFiles{
		HubCertPath:  filepath.Join(dir, "hub.crt"),
		HubKeyPath:   filepath.Join(dir, "hub.key"),
		EdgeCertPath: filepath.Join(dir, "edge.crt"),
		EdgeKeyPath:  filepath.Join(dir, "edge.key"),
		CAPath:       filepath.Join(dir, "ca.crt"),
	}
	if err := writePEMCert(files.CAPath, caDER); err != nil {
		return nil, err
	}
	if err := writePEMCert(files.HubCertPath, hubPair.certDER); err != nil {
		return nil, err
	}
	if err := writePEMKey(files.HubKeyPath, hubPair.key); err != nil {
		return nil, err
	}
	if err := writePEMCert(files.EdgeCertPath, edgePair.certDER); err != nil {
		return nil, err
	}
	if err := writePEMKey(files.EdgeKeyPath, edgePair.key); err != nil {
		return nil, err
	}
	return files, nil
}

type leafPair struct {
	certDER []byte
	key     *rsa.PrivateKey
}

func createSignedLeaf(caCert *x509.Certificate, caKey *rsa.PrivateKey, cn string, dnsSAN []string, ipSAN []net.IP, usages []x509.ExtKeyUsage) (*leafPair, error) {
	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  usages,
		DNSNames:     dnsSAN,
		IPAddresses:  ipSAN,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	return &leafPair{certDER: certDER, key: leafKey}, nil
}

func writePEMCert(path string, der []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func writePEMKey(path string, key *rsa.PrivateKey) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}
