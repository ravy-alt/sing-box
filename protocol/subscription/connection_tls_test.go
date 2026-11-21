package subscription

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func connectionTestTLS(t *testing.T) (string, string) {
	t.Helper()
	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "proxy.test"},
		DNSNames: []string{"proxy.test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &private.PublicKey, private)
	require.NoError(t, err)
	key, err := x509.MarshalPKCS8PrivateKey(private)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
	return fmt.Sprintf(`,"tls":{"enabled":true,"certificate":%q,"key":%q}`, certPEM, keyPEM),
		fmt.Sprintf(`,"tls":{"enabled":true,"server_name":"proxy.test","certificate":%q}`, certPEM)
}

func TestSubscriptionTLSConnectionProtocols(t *testing.T) {
	serverTLS, clientTLS := connectionTestTLS(t)
	cases := []connectionProtocolCase{
		{"anytls", "anytls", `"users":[{"password":"test"}]` + serverTLS, `"password":"test"` + clientTLS, false},
		{"trojan-tls", "trojan", `"users":[{"password":"test"}]` + serverTLS, `"password":"test"` + clientTLS, false},
		{"vless-tls", "vless", `"users":[{"uuid":"a40c76d3-7c47-46da-b187-1598c7c8c596"}]` + serverTLS, `"uuid":"a40c76d3-7c47-46da-b187-1598c7c8c596"` + clientTLS, false},
	}
	cases = append(cases, connectionQUICCases(serverTLS, clientTLS)...)
	testConnectionProtocols(t, cases)
}
