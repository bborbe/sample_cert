// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	libhttp "github.com/bborbe/http"
	"io"
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

	clientBuilder := libhttp.NewClientBuilder()
	clientBuilder.WithClientCert(caCertPath, clientCertPath, clientKeyPath)
	httpClient, err := clientBuilder.Build(ctx)
	if err != nil {
		return errors.Wrapf(ctx, err, "create httpClient failed")
	}

	// Make a request to an HTTPS server
	resp, err := httpClient.Get("https://localhost:8443/metrics") // Adjust URL as needed
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
