package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	"github.com/forestgiant/semver"
	"github.com/forestgiant/stela"
	"gitlab.fg/otis/iris"
	"gitlab.fg/otis/iris/pb"
	"gitlab.fg/otis/iris/store"
	"gitlab.fg/otis/iris/transport"

	fggrpclog "github.com/forestgiant/grpclog"
	fglog "github.com/forestgiant/log"
	stela_api "github.com/forestgiant/stela/api"
	iris_api "gitlab.fg/otis/iris/api"
)

const (
	version             = "0.11.0"               // version represents the semantic version of this service/api
	timeout             = 500 * time.Millisecond // default timeout for context objects
	exitStatusSuccess   = 0
	exitStatusError     = 1
	exitStatusInterrupt = 2
)

func init() {
	l := fglog.Logger{}.With("logger", "grpc")
	grpclog.SetLogger(&fggrpclog.Structured{Logger: &l})
}

func main() {
	os.Exit(run())
}

// We use the run function below instead of directly using main.  This allows
// us to properly capture the exit status while ensuring that defers are
// captured before the return
func run() (status int) {
	logger := fglog.Logger{}.With("logger", "iris", "time", fglog.DefaultTimestamp, "caller", fglog.DefaultCaller, "service", "iris")

	// Setup semantic versioning
	if err := semver.SetVersion(version); err != nil {
		logger.Error("Unable to set semantic version.", "error", err.Error())
		return exitStatusError
	}

	// Define our inputs
	var defaultCaPath = "ca.crt"
	var defaultKeyPath = "server.key"
	var defaultCertPath = "server.crt"

	var (
		insecure  = false
		nostela   = false
		stelaAddr = stela.DefaultStelaAddress

		serverName = iris.DefaultServerName
		caPath     = defaultCaPath
		keyPath    = defaultKeyPath
		certPath   = defaultCertPath

		stelaServerName = stela.DefaultServerName
		stelaCAPath     = defaultCaPath
		stelaKeyPath    = defaultKeyPath
		stelaCertPath   = defaultCertPath

		raftDir  = "raftDir"
		port     = iris.DefaultServicePort
		joinAddr = ""
	)

	// Parse, prepare, and validate inputs
	if err := prepareInputs(&port, &insecure, &nostela, &stelaAddr, &certPath, &keyPath, &caPath, &serverName, &stelaCertPath, &stelaKeyPath, &stelaCAPath, &stelaServerName, &raftDir, &joinAddr); err != nil {
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

		if insecure {
			client, err = stela_api.NewClient(ctx, stelaAddr, nil)
		} else {
			client, err = stela_api.NewTLSClient(ctx, stelaAddr, stelaServerName, stelaCertPath, stelaKeyPath, stelaCAPath)
		}

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
		if err := client.Register(registerCtx, service); err != nil {
			logger.Error("Failed to register service.", "error", err.Error())
			return exitStatusError
		}

		defer func() {
			deregisterCtx, cancelDeregister := context.WithTimeout(context.Background(), timeout)
			defer cancelDeregister()
			client.Deregister(deregisterCtx, service)
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
			// Load the certificates from disk
			certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
			if err != nil {
				errchan <- fmt.Errorf("Failed to load certificate. %s", err)
				return
			}

			// Create a certificate pool from the certificate authority
			certPool := x509.NewCertPool()
			ca, err := ioutil.ReadFile(caPath)
			if err != nil {
				errchan <- fmt.Errorf("Failed to read CA certificate. %s", err)
				return
			}

			// Append the client certificates from the CA
			if ok := certPool.AppendCertsFromPEM(ca); !ok {
				errchan <- errors.New("Failed to append client certs")
				return
			}

			// Create the TLS credentials
			creds := credentials.NewTLS(&tls.Config{
				ClientAuth:   tls.RequireAndVerifyClientCert,
				Certificates: []tls.Certificate{certificate},
				ClientCAs:    certPool,
			})

			opts = append(opts, grpc.Creds(creds))
		}

		logger.Info("Starting iris")
		grpcServer := grpc.NewServer(opts...)
		server := &transport.Server{
			Store: store,
			Proxy: &transport.Proxy{
				ServerName: serverName,
				CertPath:   certPath,
				KeyPath:    keyPath,
				CAPath:     caPath,
			},
		}
		pb.RegisterIrisServer(grpcServer, server)
		errchan <- grpcServer.Serve(l)
	}()

	// Join the raft leader if necessary
	if !startAsLeader {
		go func() {
			logger.Info("Joining raft cluster")
			if err := join(joinAddr, raftAddr, serverName, certPath, keyPath, caPath, 500*time.Millisecond); err != nil {
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

func prepareInputs(port *int, insecure *bool, nostela *bool, stelaAddr *string, certPath *string, keyPath *string, caPath *string, serverName *string, stelaCertPath *string, stelaKeyPath *string, stelaCAPath *string, stelaServerName *string, raftDir *string, joinAddr *string) error {
	// Parse command line flags
	flag.BoolVar(insecure, "insecure", *insecure, "Disable SSL, allowing unenecrypted communication with this service.")
	flag.BoolVar(nostela, "nostela", *nostela, "Disable automatic stela registration.")
	flag.StringVar(stelaAddr, "stela", *stelaAddr, "Address of the stela service you would like to use for discovery")

	flag.StringVar(certPath, "cert", *certPath, "Path to the certificate file for the server.")
	flag.StringVar(keyPath, "key", *keyPath, "Path to the private key file for the server.")
	flag.StringVar(caPath, "ca", *caPath, "Path to the certificate authority for the server.")
	flag.StringVar(serverName, "serverName", *serverName, "The common name of the server you are connecting to.")

	flag.StringVar(stelaCertPath, "stleaCert", *stelaCertPath, "Path to the certificate file for the stela server.")
	flag.StringVar(stelaKeyPath, "stelaKey", *stelaKeyPath, "Path to the private key file for the stela server.")
	flag.StringVar(stelaCAPath, "stelaCA", *stelaCAPath, "Path to the certificate authority for the stela server.")
	flag.StringVar(stelaServerName, "stelaServerName", *stelaServerName, "The common name of the stela server you are connecting to.")

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
func join(joinAddr, raftAddr string, serverName string, cert string, key string, ca string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := iris_api.NewTLSClient(ctx, joinAddr, serverName, cert, key, ca)
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
