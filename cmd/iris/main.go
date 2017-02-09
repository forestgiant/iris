package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/forestgiant/portutil"
	"github.com/forestgiant/semver"
	"gitlab.fg/go/stela"
	"gitlab.fg/otis/iris"
	"gitlab.fg/otis/iris/pb"
	"gitlab.fg/otis/iris/store"
	"gitlab.fg/otis/iris/transport"

	fglog "github.com/forestgiant/log"
	stela_api "gitlab.fg/go/stela/api"
	iris_api "gitlab.fg/otis/iris/api"
)

const (
	version         = "0.9.0"                //version represents the semantic version of this service/api
	timeout         = 500 * time.Millisecond //default timeout for context objects
	defaultRaftAddr = ":12000"
)

func main() {
	var logger = fglog.Logger{}.With("time", fglog.DefaultTimestamp, "caller", fglog.DefaultCaller, "service", "iris")

	// Set up semantic versioning
	if err := semver.SetVersion(version); err != nil {
		logger.Error("Unable to set semantic version.", "error", err.Error())
		os.Exit(1)
	}

	// Prepare default path values
	wd, err := os.Getwd()
	if err != nil {
		logger.Error("unable to get current working directory", "error", err)
	}

	var (
		insecureUsage    = "Disable SSL, allowing unenecrypted communication with this service."
		insecure         = false
		nostelaUsage     = "Disable automatic stela registration."
		nostela          bool
		certPathUsage    = "Path to the certificate file for the server."
		certPath         = filepath.Join(wd, "server.cer")
		keyPathUsage     = "Path to the private key file for the server."
		keyPath          = filepath.Join(wd, "server.key")
		raftDirUsage     = "Directory used to store raft data."
		raftDir          = filepath.Join(wd, "raftDir")
		raftAddressUsage = "Bind address used for the raft consensus mechanism."
		raftAddr         string
		joinAddressUsage = "Join address, if any, used for the raft consensus mechanism."
		joinAddr         string
	)

	// Retrieve command line flags
	flag.BoolVar(&insecure, "insecure", insecure, insecureUsage)
	flag.BoolVar(&nostela, "nostela", nostela, nostelaUsage)
	flag.StringVar(&certPath, "cert", certPath, certPathUsage)
	flag.StringVar(&keyPath, "key", keyPath, keyPathUsage)
	flag.StringVar(&raftAddr, "raftAddr", defaultRaftAddr, raftAddressUsage)
	flag.StringVar(&joinAddr, "joinAddr", joinAddr, joinAddressUsage)
	flag.StringVar(&raftDir, "raftDir", raftDir, raftDirUsage)
	flag.Parse()

	if !insecure {
		if len(certPath) == 0 {
			logger.Error("you must provide the path to an SSL certificate used to encrypt communications with this service")
			os.Exit(1)
		}

		if len(keyPath) == 0 {
			logger.Error("you must provide the path to an SSL private key used to encrypt communications with this service")
			os.Exit(1)
		}
	}

	if len(raftAddr) == 0 {
		logger.Error("You must provide the bind address to use for the raft communication.")
		os.Exit(1)
	}

	// Obtain our stela client
	var client *stela_api.Client
	if nostela == false {
		ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
		defer cancelFunc()
		client, err = stela_api.NewClient(ctx, stela.DefaultStelaAddress, certPath)
		if err != nil {
			logger.Error("Failed to obtain stela client.", "error", err.Error())
			os.Exit(1)
		}
	}

	// Determine if we should join an existing raft network
	enableSingleMode := true
	if len(joinAddr) > 0 {
		enableSingleMode = false
	} else if nostela == false {
		discoverCtx, cancelDiscover := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancelDiscover()
		services, err := client.Discover(discoverCtx, iris.DefaultServiceName)
		if err != nil {
			logger.Error("Unable to discover iris services")
			os.Exit(1)
		}

		if len(services) > 0 {
			enableSingleMode = false
			joinAddr = services[0].IPv4Address()
		}
	}
	logger.Info("Single mode determined", "enableSingleMode", enableSingleMode, "joining", joinAddr)

	// Obtain an available port
	port, err := portutil.GetUniqueTCP()
	if err != nil {
		logger.Error("unable to obtain open port", err)
		os.Exit(1)
	}
	logger = logger.With("port", port)

	// Register service with Stela api
	service := &stela.Service{
		Name: iris.DefaultServiceName,
		Port: int32(port),
	}
	if !nostela {
		registerCtx, cancelRegister := context.WithTimeout(context.Background(), timeout)
		defer cancelRegister()
		if err := client.RegisterService(registerCtx, service); err != nil {
			logger.Error("Failed to register service.", "error", err.Error())
			os.Exit(1)
		}
	}
	errchan := make(chan error)

	// Handle interrupts
	go func(client *stela_api.Client, service *stela.Service) {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		sig := <-c

		// Deregister from stela
		if !nostela {
			deregisterCtx, cancelDeregister := context.WithTimeout(context.Background(), timeout)
			defer cancelDeregister()
			client.DeregisterService(deregisterCtx, service)
		}

		errchan <- fmt.Errorf("%s", sig)
	}(client, service)

	// Serve our remote procedures
	l, err := net.Listen("tcp", service.IPv4Address())
	if err != nil {
		errchan <- err
	}

	var opts []grpc.ServerOption
	if !insecure {
		creds, err := credentials.NewServerTLSFromFile(certPath, keyPath)
		if err != nil {
			logger.Error("Failed to generate credentials.", "error", err)
			os.Exit(1)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	logger.Info("Opening data store.")
	var store = store.NewStore(raftAddr, raftDir, logger)
	if err := store.Open(enableSingleMode); err != nil {
		logger.Error("Failed to open data store.", "error", err)
		os.Exit(1)
	}

	go func() {
		logger.Info("Starting iris", "port", service.Port, "stela", !nostela, "secured", !insecure)
		grpcServer := grpc.NewServer(opts...)
		server := &transport.Server{
			Store: store,
		}
		pb.RegisterIrisServer(grpcServer, server)
		errchan <- grpcServer.Serve(l)
	}()

	if enableSingleMode == false {
		// If join was specified, make the join request.
		go func() {
			time.Sleep(3 * time.Second)
			if joinAddr != "" {
				logger.Info("Joining raft node", "joinAddr", joinAddr, "raftAddr", raftAddr)
				if err := join(joinAddr, raftAddr); err != nil {
					logger.Error("Failed to join raft node", "joinAddr", joinAddr, "raftAddr", raftAddr, "error", err.Error())
					os.Exit(1)
				}
			}
		}()
	}

	logger.Error("exiting", "error", (<-errchan).Error())
}

func join(joinAddr, raftAddr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	client, err := iris_api.NewTLSClient(ctx, joinAddr, "iris.forestgiant.com", "ca.cer")
	if err != nil {
		return err
	}

	joinCtx, cancelJoin := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelJoin()

	if err := client.Join(joinCtx, raftAddr); err != nil {
		return err
	}

	return nil
}
