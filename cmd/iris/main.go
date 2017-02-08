package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
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
)

const (
	version         = "0.9.0"                //version represents the semantic version of this service/api
	timeout         = 500 * time.Millisecond //default timeout for context objects
	defaultRaftAddr = ":12000"
)

func main() {
	var logger = fglog.Logger{}.With("time", fglog.DefaultTimestamp, "caller", fglog.DefaultCaller, "service", "iris")

	// Set up semantic versioning
	err := semver.SetVersion(version)
	if err != nil {
		logger.Error("Unable to set semantic version.", "error", err.Error())
		os.Exit(1)
	}

	// Set up the default certificate path
	wd, err := os.Getwd()
	if err != nil {
		logger.Error("unable to get current working directory", "error", err)
	}
	defaultCertPath := filepath.Join(wd, "server.cer")
	defaultKeyPath := filepath.Join(wd, "server.key")
	defaultRaftDirPath := filepath.Join(wd, "raftDir")

	// Retrieve command line flags
	var (
		insecureUsage    = "Disable SSL, allowing unenecrypted communication with this service."
		insecurePtr      = flag.Bool("insecure", false, insecureUsage)
		certFileUsage    = "Path to the certificate file for the server."
		certFilePtr      = flag.String("cert", defaultCertPath, certFileUsage)
		keyFileUsage     = "Path to the private key file for the server."
		keyFilePtr       = flag.String("key", defaultKeyPath, keyFileUsage)
		nostelaUsage     = "Disable automatic stela registration."
		noStelaPtr       = flag.Bool("nostela", false, nostelaUsage)
		raftAddressUsage = "Bind address used for the raft consensus mechanism."
		raftAddrPtr      = flag.String("raftAddr", defaultRaftAddr, raftAddressUsage)
		joinAddressUsage = "Join address, if any, used for the raft consensus mechanism."
		joinAddrPtr      = flag.String("joinAddr", "", joinAddressUsage)
		raftDirUsage     = "Directory used to store raft data."
		raftDirPtr       = flag.String("raftDir", defaultRaftDirPath, raftDirUsage)
	)
	flag.Parse()

	var cert string
	var key string
	if !*insecurePtr {
		if len(*certFilePtr) == 0 {
			logger.Error("you must provide the path to an SSL certificate used to encrypt communications with this service")
			os.Exit(1)
		} else {
			cert = *certFilePtr
		}

		if len(*keyFilePtr) == 0 {
			logger.Error("you must provide the path to an SSL private key used to encrypt communications with this service")
			os.Exit(1)
		} else {
			key = *keyFilePtr
		}
	}

	if len(*raftAddrPtr) == 0 {
		logger.Error("You must provide the bind address to use for the raft consensus mechanism")
		os.Exit(1)
	}

	// Obtain an available port
	port, err := portutil.GetUniqueTCP()
	if err != nil {
		logger.Error("unable to obtain open port", err)
		os.Exit(1)
	}
	logger = logger.With("port", port)

	// Register service with Stela api
	var client *stela_api.Client
	service := &stela.Service{
		Name: iris.DefaultServiceName,
		Port: int32(port),
	}
	if !*noStelaPtr {
		ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
		defer cancelFunc()
		client, err = stela_api.NewClient(ctx, stela.DefaultStelaAddress, cert)
		if err != nil {
			logger.Error("Failed to obtain stela client.", "error", err.Error())
			os.Exit(1)
		}

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
		if !*noStelaPtr {
			deregisterCtx, cancelDeregister := context.WithTimeout(context.Background(), timeout)
			defer cancelDeregister()
			client.DeregisterService(deregisterCtx, service)
		}

		errchan <- fmt.Errorf("%s", sig)
	}(client, service)

	// Serve our remote procedures
	go func() {
		l, err := net.Listen("tcp", service.IPv4Address())
		if err != nil {
			errchan <- err
		}

		var opts []grpc.ServerOption
		if !*insecurePtr {
			creds, err := credentials.NewServerTLSFromFile(cert, key)
			if err != nil {
				logger.Error("Failed to generate credentials.", "error", err)
				os.Exit(1)
			}
			opts = append(opts, grpc.Creds(creds))
		}

		logger.Info("Opening data store.")
		var store = store.NewStore(*raftAddrPtr, *raftDirPtr, logger)
		if err := store.Open(*joinAddrPtr == ""); err != nil {
			logger.Error("Failed to open data store.", "error", err)
			os.Exit(1)
		}

		// If join was specified, make the join request.
		if *joinAddrPtr != "" {
			logger.Info("Joining raft node", "joinAddr", *joinAddrPtr, "raftAddr", *raftAddrPtr)
			if err := join(*joinAddrPtr, *raftAddrPtr); err != nil {
				logger.Error("Failed to join raft node", "joinAddr", *joinAddrPtr, "raftAddr", *raftAddrPtr, "error", err)
				os.Exit(1)
			}
		}

		logger.Info("Starting iris", "port", service.Port, "stela", !*noStelaPtr, "secured", !*insecurePtr)
		grpcServer := grpc.NewServer(opts...)
		server := &transport.Server{
			Store: store,
		}
		pb.RegisterIrisServer(grpcServer, server)
		errchan <- grpcServer.Serve(l)
	}()

	logger.Error("exiting", "error", (<-errchan).Error())
}

func join(joinAddr, raftAddr string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr})
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
