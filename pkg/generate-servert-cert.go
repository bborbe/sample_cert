// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"time"

	"github.com/bborbe/errors"
	"github.com/golang/glog"
)

// GenerateServerCert generates a server certificate signed by the given CA.
func GenerateServerCert(ctx context.Context, caCertPath string, caKeyPath string, serverCertPath string, serverKeyPath string) error {
	// Load the CA certificate and private key
	caCert, caKey, err := LoadCACertificate(ctx, caCertPath, caKeyPath)
	if err != nil {
		return errors.Wrapf(ctx, err, "Failed to load CA certificate or key")
	}

	// Generate server private key
	serverPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	// Create server certificate template
	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour) // 1 year validity

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return err
	}

	serverCertTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"My Server Organization"},
			CommonName:   "localhost",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")}, // Adjust IPs as needed
		DNSNames:              []string{"localhost"},
	}

	// Sign the server certificate with the CA
	serverCertDER, err := x509.CreateCertificate(rand.Reader, &serverCertTemplate, caCert, &serverPriv.PublicKey, caKey)
	if err != nil {
		return err
	}

	// Write server certificate to file
	certOut, err := os.Create(serverCertPath)
	if err != nil {
		return err
	}
	defer certOut.Close()
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER}); err != nil {
		return err
	}
	glog.V(2).Infof("Server certificate written to server_cert.pem")

	// Write server private key to file
	keyOut, err := os.Create(serverKeyPath)
	if err != nil {
		return err
	}
	defer keyOut.Close()
	serverPrivBytes, err := x509.MarshalECPrivateKey(serverPriv)
	if err != nil {
		return err
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: serverPrivBytes}); err != nil {
		return err
	}

	glog.V(2).Infof("Server private key written to server_key.pem")

	return nil
}
