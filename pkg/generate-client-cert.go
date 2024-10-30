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
	"os"
	"time"

	"github.com/bborbe/errors"
	"github.com/golang/glog"
)

// GenerateClientCert generates a client certificate signed by the given CA.
func GenerateClientCert(ctx context.Context, caCertPath string, caKeyPath string, clientCertPath string, clientKeyPath string) error {
	// Load the CA certificate and private key
	caCert, caKey, err := LoadCACertificate(ctx, caCertPath, caKeyPath)
	if err != nil {
		return errors.Wrapf(ctx, err, "Failed to load CA certificate or key")
	}

	// Generate client private key
	clientPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	// Create client certificate template
	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour) // 1 year validity

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return err
	}

	clientCertTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"My Client Organization"},
			CommonName:   "client", // Adjust as necessary for client identity
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	// Sign the client certificate with the CA
	clientCertDER, err := x509.CreateCertificate(rand.Reader, &clientCertTemplate, caCert, &clientPriv.PublicKey, caKey)
	if err != nil {
		return err
	}

	// Write client certificate to file
	certOut, err := os.Create(clientCertPath)
	if err != nil {
		return err
	}
	defer certOut.Close()
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER}); err != nil {
		return err
	}
	glog.V(2).Infof("Client certificate written to client_cert.pem")

	// Write client private key to file
	keyOut, err := os.Create(clientKeyPath)
	if err != nil {
		return err
	}
	defer keyOut.Close()
	clientPrivBytes, err := x509.MarshalECPrivateKey(clientPriv)
	if err != nil {
		return err
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: clientPrivBytes}); err != nil {
		return err
	}
	glog.V(2).Infof("Client private key written to client_key.pem")
	return nil
}
