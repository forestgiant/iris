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
	"gitlab.fg/otis/iris"
	"gitlab.fg/otis/iris/pb"
	"gitlab.fg/otis/iris/store"
	"gitlab.fg/otis/iris/transport"

	fglog "github.com/forestgiant/log"
	"gitlab.fg/go/stela"
	stela_api "gitlab.fg/go/stela/api"
	iris_api "gitlab.fg/otis/iris/api"
)

const (
	version           = "0.9.0"                //version represents the semantic version of this service/api
	timeout           = 500 * time.Millisecond //default timeout for context objects
	exitStatusSuccess = 0
	exitStatusError   = 1
)

func main() {
	os.Exit(run())
}

func run() (status int) {
	// Setup a logger to use
	var logger = fglog.Logger{}.With("time", fglog.DefaultTimestamp, "caller", fglog.DefaultCaller, "service", "iris")

	// Setup semantic versioning
	if err := semver.SetVersion(version); err != nil {
		logger.Error("Unable to set semantic version.", "error", err.Error())
		return exitStatusError
	}

	// Prepare default path values
	wd, err := os.Getwd()
	if err != nil {
		logger.Error("unable to get current working directory", "error", err)
		return exitStatusError
	}

	// Define and parse our command line flags
	const (
		insecureParam    = "insecure"
		insecureUsage    = "Disable SSL, allowing unenecrypted communication with this service."
		noStelaParam     = "nostela"
		nostelaUsage     = "Disable automatic stela registration."
		stelaAddrParam   = "stela"
		stelaAddrUsage   = "Address of the stela service you would like to use for discovery"
		certPathParam    = "cert"
		certPathUsage    = "Path to the certificate file for the server."
		keyPathParam     = "key"
		keyPathUsage     = "Path to the private key file for the server."
		raftDirParam     = "raftdir"
		raftDirUsage     = "Directory used to store raft data."
		raftPortParam    = "raft"
		raftPortUsage    = "Port used for raft communications."
		joinAddrParam    = "join"
		joinAddressUsage = "Address of the raft cluster you would like to join."
	)

	var (
		insecure  = false
		nostela   = false
		stelaAddr = stela.DefaultStelaAddress
		certPath  = filepath.Join(wd, "server.cer")
		keyPath   = filepath.Join(wd, "server.key")
		raftDir   = filepath.Join(wd, "raftDir")
		raftPort  = 0
		joinAddr  = ""
	)

	flag.BoolVar(&insecure, insecureParam, insecure, insecureUsage)
	flag.BoolVar(&nostela, noStelaParam, nostela, nostelaUsage)
	flag.StringVar(&stelaAddr, stelaAddrParam, stelaAddr, stelaAddrUsage)
	flag.StringVar(&certPath, certPathParam, certPath, certPathUsage)
	flag.StringVar(&keyPath, keyPathParam, keyPath, keyPathUsage)
	flag.IntVar(&raftPort, raftPortParam, raftPort, raftPortUsage)
	flag.StringVar(&raftDir, raftDirParam, raftDir, raftDirUsage)
	flag.StringVar(&joinAddr, joinAddrParam, joinAddr, joinAddressUsage)
	flag.Parse()

	logger = logger.With("stela", !nostela, "secured", !insecure)

	// Validate authentication inputs
	if !insecure && len(certPath) == 0 {
		logger.Error("you must provide the path to an SSL certificate used to encrypt communications with this service")
		return exitStatusError
	}

	if !insecure && len(keyPath) == 0 {
		logger.Error("you must provide the path to an SSL private key used to encrypt communications with this service")
		return exitStatusError
	}

	// Determine raft communication port
	if raftPort == 0 {
		p, err := portutil.GetUniqueTCP()
		if err != nil {
			logger.Error("Unable to obtain open port for raft communication.", err)
			return exitStatusError
		}
		raftPort = p
	}
	raftAddress := fmt.Sprintf(":%d", raftPort)
	logger = logger.With("raftAddress", raftAddress)

	// Obtain our stela client
	var client *stela_api.Client
	if nostela == false {
		ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
		defer cancelFunc()
		client, err = stela_api.NewClient(ctx, stelaAddr, certPath)
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
		//Check for peer to join
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

	if enableSingleMode == false {
		logger = logger.With(joinAddrParam, joinAddr)
	}

	// Obtain an available port for grpc service communications
	port, err := portutil.GetUniqueTCP()
	if err != nil {
		logger.Error("unable to obtain open port for grpc communications.", err)
		return exitStatusError
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
			return exitStatusError
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
			return exitStatusError
		}
		opts = append(opts, grpc.Creds(creds))
	}

	// Setup our data store
	var store = store.NewStore(fmt.Sprintf(":%d", raftPort), raftDir, logger)
	if err := store.Open(enableSingleMode); err != nil {
		logger.Error("Failed to open data store.", "error", err)
		return exitStatusError
	}

	go func() {
		logger.Info("Starting iris")
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
			//TODO: Lets not do this sleep business
			time.Sleep(3 * time.Second)
			if joinAddr != "" {
				logger.Info("Joining raft node")
				if err := join(joinAddr, raftAddress); err != nil {
					logger.Error("Failed to join raft node", "error", err.Error())
					errchan <- err
				}
			}
		}()
	}

	logger.Error("exiting", "error", (<-errchan).Error())
	return exitStatusError
}

// join the specified raft cluster
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
