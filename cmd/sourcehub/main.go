package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/forestgiant/portutil"
	"github.com/forestgiant/semver"
	"gitlab.fg/go/stela"
	"gitlab.fg/otis/sourcehub"
	"gitlab.fg/otis/sourcehub/server"

	fglog "github.com/forestgiant/log"
	stela_api "gitlab.fg/go/stela/api"
)

const (
	version = "0.0.1"                //version represents the semantic version of this service/api
	timeout = 500 * time.Millisecond //default timeout for context objects
)

func main() {
	var logger = fglog.Logger{}.With("time", fglog.DefaultTimestamp, "caller", fglog.DefaultCaller, "service", "source-hub")

	// Set up semantic versioning
	err := semver.SetVersion(version)
	if err != nil {
		logger.Error("Unable to set semantic version.", "error", err.Error())
		os.Exit(1)
	}

	// Obtain an available port
	port, err := portutil.GetUniqueTCP()
	if err != nil {
		logger.Error("unable to obtain open port")
		os.Exit(1)
	}
	logger = logger.With("port", port)

	// Register service with Stela api
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	client, err := stela_api.NewClient(ctx, stela.DefaultStelaAddress, "")
	if err != nil {
		logger.Error("Failed to obtain stela client.", "error", err.Error())
		os.Exit(1)
	}

	service := new(stela.Service)
	service.Name = sourcehub.DefaultServiceName
	service.Port = int32(port)

	registerCtx, cancelRegister := context.WithTimeout(context.Background(), timeout)
	defer cancelRegister()
	if err := client.RegisterService(registerCtx, service); err != nil {
		logger.Error("Failed to register service.", "error", err.Error())
		os.Exit(1)
	}

	errchan := make(chan error)

	// Handle interrupts
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		sig := <-c

		// Deregister from stela
		deregisterCtx, cancelDeregister := context.WithTimeout(context.Background(), timeout)
		defer cancelDeregister()
		client.DeregisterService(deregisterCtx, service)
		errchan <- fmt.Errorf("%s", sig)
	}()

	// Serve our remote procedures
	go func() {
		logger.Info("Starting sourcehub server", "port", service.Port)
		l, err := net.Listen("tcp", fmt.Sprintf(":%d", service.Port))
		if err != nil {
			errchan <- err
		}

		grpcServer := grpc.NewServer()
		sourcehub.RegisterSourceHubServer(grpcServer, &server.Server{})
		errchan <- grpcServer.Serve(l)
	}()

	logger.Error("exiting", "error", (<-errchan).Error())
}
