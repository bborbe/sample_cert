// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"

	"github.com/bborbe/errors"
	libsentry "github.com/bborbe/sentry"
	"github.com/bborbe/service"
)

func main() {
	app := &application{}
	os.Exit(service.Main(context.Background(), app, &app.SentryDSN, &app.SentryProxy))
}

type application struct {
	SentryDSN   string `required:"false" arg:"sentry-dsn" env:"SENTRY_DSN" usage:"SentryDSN" display:"length"`
	SentryProxy string `required:"false" arg:"sentry-proxy" env:"SENTRY_PROXY" usage:"Sentry Proxy"`
	DataDir     string `required:"true" arg:"datadir" env:"DATADIR" usage:"data directory"`
	Listen      string `required:"true" arg:"listen" env:"LISTEN" usage:"address to listen to"`
}

func (a *application) Run(ctx context.Context, sentryClient libsentry.Client) error {
	caCertPath, err := filepath.Abs(path.Join(a.DataDir, "ca_cert.pem"))
	if err != nil {
		return errors.Wrapf(ctx, err, "generate caCert path failed")
	}
	clientCertPath, err := filepath.Abs(path.Join(a.DataDir, "client_cert.pem"))
	if err != nil {
		return errors.Wrapf(ctx, err, "generate clientKey path failed")
	}
	clientKeyPath, err := filepath.Abs(path.Join(a.DataDir, "client_key.pem"))
	if err != nil {
		return errors.Wrapf(ctx, err, "generate clientKey path failed")
	}

	// Load the client certificate and private key
	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		return errors.Wrapf(ctx, err, "load client certificate and key failed")
	}

	// Load the CA certificate to verify the server
	caCertPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		return errors.Wrapf(ctx, err, "read CA certificate failed")
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCertPEM); !ok {
		return errors.Wrapf(ctx, err, "append CA certificate to pool failed")
	}

	// Set up TLS configuration with the client certificate and CA
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: false, // Ensures server certificate is verified
	}

	// Create an HTTP client with the TLS configuration
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}

	// Make a request to an HTTPS server
	resp, err := client.Get("https://localhost:8443/metrics") // Adjust URL as needed
	if err != nil {
		return errors.Wrapf(ctx, err, "Failed to make request: %v", err)
	}
	defer resp.Body.Close()

	// Read and print the response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(ctx, err, "Failed to read response body: %v", err)
	}
	fmt.Printf("Response: %s\n", body)
	return nil
}
