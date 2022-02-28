package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/gtrace"
	"github.com/DataWorkbench/common/metrics"
	"github.com/DataWorkbench/common/utils/buildinfo"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/xgo/service/pbsvcstoreio"
	"github.com/DataWorkbench/resourcemanager/config"
	"github.com/DataWorkbench/resourcemanager/controller"
	"github.com/DataWorkbench/resourcemanager/options"
)

// Start for start the http server
func Start() (err error) {
	fmt.Printf("%s pid=%d program_build_info: %s\n",
		time.Now().Format(time.RFC3339Nano), os.Getpid(), buildinfo.JSONString)

	var cfg *config.Config

	cfg, err = config.Load()
	if err != nil {
		return
	}

	// init parent logger
	lp := glog.NewDefault().WithLevel(glog.Level(cfg.LogLevel))
	ctx := glog.WithContext(context.Background(), lp)

	var (
		rpcServer    *grpcwrap.Server
		metricServer *metrics.Server
		tracer       gtrace.Tracer
		tracerCloser io.Closer
	)

	defer func() {
		rpcServer.GracefulStop()
		_ = metricServer.Shutdown(ctx)

		_ = options.Close()

		if tracerCloser != nil {
			_ = tracerCloser.Close()
		}
		_ = lp.Close()
	}()

	tracer, tracerCloser, err = gtrace.New(cfg.Tracer)
	if err != nil {
		return
	}
	ctx = gtrace.ContextWithTracer(ctx, tracer)

	if err = options.Init(ctx, cfg); err != nil {
		return
	}

	// init prometheus server
	metricServer, err = metrics.NewServer(ctx, cfg.MetricsServer)
	if err != nil {
		return err
	}

	rpcServer, err = grpcwrap.NewServer(ctx, cfg.GRPCServer)
	if err != nil {
		return
	}

	rpcServer.RegisterService(&pbsvcstoreio.StoreIO_ServiceDesc, &controller.StoreIo{})

	// handle signal
	sigGroup := []os.Signal{syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM}
	sigChan := make(chan os.Signal, len(sigGroup))
	signal.Notify(sigChan, sigGroup...)

	blockChan := make(chan struct{})

	// run grpc server
	go func() {
		_ = rpcServer.ListenAndServe()
		blockChan <- struct{}{}
	}()

	go func() {
		// Ignore metrics server error.
		_ = metricServer.ListenAndServe()
	}()

	go func() {
		sig := <-sigChan
		lp.Info().String("receive system signal", sig.String()).Fire()
		blockChan <- struct{}{}
	}()

	<-blockChan
	return
}
