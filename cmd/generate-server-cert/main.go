// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"
	"path"
	"path/filepath"

	"github.com/bborbe/errors"
	"github.com/bborbe/sample_cert/pkg"
	libsentry "github.com/bborbe/sentry"
	"github.com/bborbe/service"
	"github.com/golang/glog"
)

func main() {
	app := &application{}
	os.Exit(service.Main(context.Background(), app, &app.SentryDSN, &app.SentryProxy))
}

type application struct {
	SentryDSN   string `required:"false" arg:"sentry-dsn" env:"SENTRY_DSN" usage:"SentryDSN" display:"length"`
	SentryProxy string `required:"false" arg:"sentry-proxy" env:"SENTRY_PROXY" usage:"Sentry Proxy"`
	DataDir     string `required:"true" arg:"datadir" env:"DATADIR" usage:"data directory"`
}

func (a *application) Run(ctx context.Context, sentryClient libsentry.Client) error {
	caCertPath, err := filepath.Abs(path.Join(a.DataDir, "ca_cert.pem"))
	if err != nil {
		return errors.Wrapf(ctx, err, "generate caCert path failed")
	}
	caKeyPath, err := filepath.Abs(path.Join(a.DataDir, "ca_key.pem"))
	if err != nil {
		return errors.Wrapf(ctx, err, "generate caKey path failed")
	}
	serverCertPath, err := filepath.Abs(path.Join(a.DataDir, "server_cert.pem"))
	if err != nil {
		return errors.Wrapf(ctx, err, "generate serverKey path failed")
	}
	serverKeyPath, err := filepath.Abs(path.Join(a.DataDir, "server_key.pem"))
	if err != nil {
		return errors.Wrapf(ctx, err, "generate serverKey path failed")
	}

	// Generate the server certificate signed by the CA
	if err := pkg.GenerateServerCert(ctx, caCertPath, caKeyPath, serverCertPath, serverKeyPath); err != nil {
		return errors.Wrapf(ctx, err, "Failed to generate server certificate")
	}
	glog.V(2).Infof("generate server cert(%s) and key(%s) completed", serverCertPath, serverKeyPath)

	return nil
}
