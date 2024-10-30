// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"net/http"
	"os"
	"path"
	"path/filepath"

	"github.com/bborbe/errors"
	libhttp "github.com/bborbe/http"
	libsentry "github.com/bborbe/sentry"
	"github.com/bborbe/service"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/bborbe/run"
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
	return service.Run(
		ctx,
		a.createHttpServer(),
	)
}

func (a *application) createHttpServer() run.Func {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		router := mux.NewRouter()
		router.Path("/healthz").Handler(libhttp.NewPrintHandler("OK"))
		router.Path("/readiness").Handler(libhttp.NewPrintHandler("OK"))
		router.Path("/metrics").Handler(promhttp.Handler())

		router.Path("/testloglevel").Handler(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
			glog.Errorf("error")
			glog.Warningf("warn")
			glog.V(0).Infof("info 0")
			glog.V(1).Infof("info 1")
			glog.V(2).Infof("info 2")
			glog.V(3).Infof("info 3")
			glog.V(4).Infof("info 4")
			libhttp.WriteAndGlog(resp, "test loglevel completed")
		}))

		serverCertPath, err := filepath.Abs(path.Join(a.DataDir, "server_cert.pem"))
		if err != nil {
			return errors.Wrapf(ctx, err, "generate serverKey path failed")
		}
		serverKeyPath, err := filepath.Abs(path.Join(a.DataDir, "server_key.pem"))
		if err != nil {
			return errors.Wrapf(ctx, err, "generate serverKey path failed")
		}

		glog.V(2).Infof("starting http server listen on %s", a.Listen)
		return libhttp.NewServerTLS(
			a.Listen,
			router,
			serverCertPath,
			serverKeyPath,
		).Run(ctx)
	}
}
