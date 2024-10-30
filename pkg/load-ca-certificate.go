// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"

	"github.com/bborbe/errors"
)

// LoadCACertificate loads a CA certificate and private key from files.
func LoadCACertificate(ctx context.Context, certPath, keyPath string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	var err error
	certPath, err = filepath.Abs(certPath)
	if err != nil {
		return nil, nil, errors.Wrapf(ctx, err, "abs certPath failed")
	}
	keyPath, err = filepath.Abs(keyPath)
	if err != nil {
		return nil, nil, errors.Wrapf(ctx, err, "abs keyPath failed")
	}

	// Read CA certificate
	caCertPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, nil, err
	}
	block, _ := pem.Decode(caCertPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, nil, err
	}
	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, err
	}

	// Read CA private key
	caKeyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, nil, err
	}
	block, _ = pem.Decode(caKeyPEM)
	if block == nil || block.Type != "EC PRIVATE KEY" {
		return nil, nil, err
	}
	caKey, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, nil, err
	}

	return caCert, caKey, nil
}
