package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

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
	version             = "0.11.0"               // version represents the semantic version of this service/api
	timeout             = 500 * time.Millisecond // default timeout for context objects
	exitStatusSuccess   = 0
	exitStatusError     = 1
	exitStatusInterrupt = 2
)

func main() {
	os.Exit(run())
}

// We use the run function below instead of directly using main.  This allows
// us to properly capture the exit status while ensuring that defers are
// captured before the return
func run() (status int) {
	logger := fglog.Logger{}.With("time", fglog.DefaultTimestamp, "caller", fglog.DefaultCaller, "service", "iris")

	// Setup semantic versioning
	if err := semver.SetVersion(version); err != nil {
		logger.Error("Unable to set semantic version.", "error", err.Error())
		return exitStatusError
	}

	// Define our inputs
	var (
		insecure  = false
		nostela   = false
		stelaAddr = stela.DefaultStelaAddress
		certPath  = "server.cer"
		keyPath   = "server.key"
		raftDir   = "raftDir"
		port      = iris.DefaultServicePort
		joinAddr  = ""
	)

	// Parse, prepare, and validate inputs
	if err := prepareInputs(&port, &insecure, &nostela, &stelaAddr, &certPath, &keyPath, &raftDir, &joinAddr); err != nil {
		logger.Error("Error parsing inputs.", "error", err.Error())
		return exitStatusError
	}
	logger = logger.With("stela", !nostela, "secured", !insecure)

	// Obtain our stela client
	var client *stela_api.Client
	var err error
	if !nostela {
		ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
		defer cancelFunc()
		client, err = stela_api.NewClient(ctx, stelaAddr, certPath)
		if err != nil {
			logger.Error("Failed to obtain stela client.", "error", err.Error())
			os.Exit(1)
		}
		defer client.Close()
	}

	// Set up our grpc service parameters
	raftPort := port + 1
	service := &stela.Service{
		Name: iris.DefaultServiceName,
		Port: int32(port),
	}

	// Determine join address before registering our service
	// Important not to discover ourselves as a node to join
	if len(joinAddr) == 0 && !nostela {
		joinAddr, err = fetchJoinAddress(client)
	}
	startAsLeader := len(joinAddr) == 0
	if !startAsLeader {
		logger = logger.With("join", joinAddr)
	}

	// Register service with Stela api
	if !nostela {
		registerCtx, cancelRegister := context.WithTimeout(context.Background(), timeout)
		defer cancelRegister()
		if err := client.RegisterService(registerCtx, service); err != nil {
			logger.Error("Failed to register service.", "error", err.Error())
			return exitStatusError
		}

		defer func() {
			deregisterCtx, cancelDeregister := context.WithTimeout(context.Background(), timeout)
			defer cancelDeregister()
			client.DeregisterService(deregisterCtx, service)
		}()
	}

	// Determine grpc and raft addr
	host, _, err := net.SplitHostPort(service.IPv4Address())
	if err != nil {
		logger.Error("Unable to determine grpc address.", err)
		return exitStatusError
	}
	grpcAddr := net.JoinHostPort(host, strconv.Itoa(port))
	raftAddr := net.JoinHostPort(host, strconv.Itoa(raftPort))
	logger = logger.With("raftAddr", raftAddr, "grpcAddr", grpcAddr)

	// Setup our data store
	store := store.NewStore(raftAddr, raftDir, logger)
	if err := store.Open(startAsLeader); err != nil {
		logger.Error("Failed to open data store.", "error", err)
		return exitStatusError
	}

	// Flow control
	errchan := make(chan error)
	intchan := make(chan int)
	go func() {
		intchan <- handleInterrupts()
	}()

	// Serve our remote procedures
	go func() {
		l, err := net.Listen("tcp", service.IPv4Address())
		if err != nil {
			errchan <- fmt.Errorf("Failed to start tcp listener. %s", err)
		}

		var opts []grpc.ServerOption
		if !insecure {
			creds, err := credentials.NewServerTLSFromFile(certPath, keyPath)
			if err != nil {
				errchan <- fmt.Errorf("Failed to generate credentials. %s", err)
				return
			}
			opts = append(opts, grpc.Creds(creds))
		}

		logger.Info("Starting iris")
		grpcServer := grpc.NewServer(opts...)
		server := &transport.Server{
			Store: store,
		}
		pb.RegisterIrisServer(grpcServer, server)
		errchan <- grpcServer.Serve(l)
	}()

	// Join the raft leader if necessary
	if !startAsLeader {
		go func() {
			logger.Info("Joining raft cluster")
			if err := join(joinAddr, raftAddr, 500*time.Millisecond); err != nil {
				errchan <- fmt.Errorf("Failed to join raft cluster. %s", err)
			}
		}()
	}

	// Wait for our exit signals
	select {
	case err := <-errchan:
		logger.Error("Exiting.", "error", err.Error())
		return exitStatusError
	case status := <-intchan:
		logger.Info("Interrupted")
		return status
	}
}

func fetchJoinAddress(client *stela_api.Client) (string, error) {
	discoverCtx, cancelDiscover := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelDiscover()
	services, err := client.Discover(discoverCtx, iris.DefaultServiceName)
	if err != nil {
		return "", err
	}

	if len(services) == 0 {
		return "", fmt.Errorf("Discover request returned no services matching %s", iris.DefaultServiceName)
	}
	return services[0].IPv4Address(), nil
}

func prepareInputs(port *int, insecure *bool, nostela *bool, stelaAddr *string, certPath *string, keyPath *string, raftDir *string, joinAddr *string) error {
	// Parse command line flags
	flag.BoolVar(insecure, "insecure", *insecure, "Disable SSL, allowing unenecrypted communication with this service.")
	flag.BoolVar(nostela, "nostela", *nostela, "Disable automatic stela registration.")
	flag.StringVar(stelaAddr, "stela", *stelaAddr, "Address of the stela service you would like to use for discovery")
	flag.StringVar(certPath, "cert", *certPath, "Path to the certificate file for the server.")
	flag.StringVar(keyPath, "key", *keyPath, "Path to the private key file for the server.")
	flag.IntVar(port, "port", *port, "Port used for grpc communications.")
	flag.StringVar(raftDir, "raftdir", *raftDir, "Directory used to store raft data.")
	flag.StringVar(joinAddr, "join", *joinAddr, "Address of the raft cluster leader you would like to join.")
	flag.Parse()

	// Validate authentication inputs
	if !*insecure && len(*certPath) == 0 {
		return errors.New("You must provide the path to an SSL certificate used to encrypt communications with this service")
	}

	if !*insecure && len(*keyPath) == 0 {
		return errors.New("You must provide the path to an SSL private key used to encrypt communications with this service")
	}

	return nil
}

// join the specified raft cluster
func join(joinAddr, raftAddr string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := iris_api.NewTLSClient(ctx, joinAddr, "iris.forestgiant.com", "ca.cer")
	if err != nil {
		return err
	}

	if err := client.Join(ctx, raftAddr); err != nil {
		return err
	}

	return nil
}

// listen for interrupt notifications and return when they have been received
func handleInterrupts() int {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
	return exitStatusInterrupt
}
