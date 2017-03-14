package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/forestgiant/iris"
	"github.com/forestgiant/iris/api"

	fglog "github.com/forestgiant/log"
	"github.com/forestgiant/stela"
	stela_api "github.com/forestgiant/stela/api"
)

const (
	setCommandName          = "set"
	getCommandName          = "get"
	getSourcesCommandName   = "getsources"
	getKeysCommandName      = "getkeys"
	removeSourceCommandName = "removesource"
	removeValueCommandName  = "removekey"

	sourceUsage   = "The name of the source to be used."
	sourceParam   = "source"
	keyUsage      = "The name of the key to be used."
	keyParam      = "key"
	valueUsage    = "The value to be used."
	valueParam    = "value"
	addrUsage     = "Address of the stela server to connect to."
	addrParam     = "addr"
	insecureUsage = "Disable SSL, allowing unenecrypted communication with the service."
	insecureParam = "insecure"
	noStelaUsage  = "Disable usage of Stela for service discovery."
	noStelaParam  = "nostela"

	serverNameUsage = "The common name of the server you would like to connect to."
	serverNameParam = "serverName"
	clientCertUsage = "Path to the certificate file for the client."
	clientCertParam = "clientCert"
	clientKeyUsage  = "Path to the private key file for the client."
	clientKeyParam  = "clientKey"
	caPathUsage     = "Path to the certificate authority you would like to use."
	caPathParam     = "ca"

	stelaServerNameUsage = "The common name of the stela server you would like to connect to."
	stelaServerNameParam = "stelaServerName"
	stelaCertUsage       = "Path to the certificate file for the stela server."
	stelaCertParam       = "stelaCert"
	stelaKeyUsage        = "Path to the private key file for the client."
	stelaKeyParam        = "stelaKey"
	stelaCAPathUsage     = "Path to the certificate authority you would like to use."
	stelaCAPathParam     = "stelaCA"

	exitStatusSuccess = 0
	exitStatusError   = 1
)

func printUsageInstructions() {
	fmt.Println("usage: iris-cli <command> [<args>]")
	fmt.Println("The available commands are: ")
	fmt.Printf("\t%s\t\t\tSet a value\n", setCommandName)
	fmt.Printf("\t%s\t\t\tGet a value\n", getCommandName)
	fmt.Printf("\t%s\t\tGet a list of available sources\n", getSourcesCommandName)
	fmt.Printf("\t%s\t\t\tGet a list of keys contained in a source\n", getKeysCommandName)
	fmt.Printf("\t%s\t\tRemove a source\n", removeSourceCommandName)
	fmt.Printf("\t%s\t\tRemove a key/value pair\n", removeValueCommandName)
}

func main() {
	os.Exit(run())
}

func run() (status int) {
	logger := fglog.Logger{}.With("time", fglog.DefaultTimestamp)

	var defaultCertPath = "client.crt"
	var defaultKeyPath = "client.key"
	var defaultCaPath = "ca.crt"

	var (
		addr     string
		command  string
		source   string
		key      string
		value    string
		insecure = false
		noStela  = false

		serverName = iris.DefaultServerName
		clientCert = defaultCertPath
		clientKey  = defaultKeyPath
		ca         = defaultCaPath

		stelaServerName = stela.DefaultServerName
		stelaCert       = defaultCertPath
		stelaKey        = defaultKeyPath
		stelaCA         = defaultCaPath
	)

	if len(os.Args) <= 1 {
		printUsageInstructions()
		return exitStatusError
	}

	command = os.Args[1]
	if command != setCommandName &&
		command != getCommandName &&
		command != getSourcesCommandName &&
		command != getKeysCommandName &&
		command != removeSourceCommandName &&
		command != removeValueCommandName {
		printUsageInstructions()
		return exitStatusError
	}

	flag := flag.NewFlagSet(command, flag.ExitOnError)
	flag.StringVar(&addr, addrParam, addr, addrUsage)
	flag.StringVar(&source, sourceParam, source, sourceUsage)
	flag.StringVar(&key, keyParam, key, keyUsage)
	flag.StringVar(&value, valueParam, value, valueUsage)
	flag.BoolVar(&insecure, insecureParam, insecure, insecureUsage)
	flag.BoolVar(&noStela, noStelaParam, noStela, noStelaUsage)

	flag.StringVar(&clientCert, clientCertParam, clientCert, clientCertUsage)
	flag.StringVar(&clientKey, clientKeyParam, clientKey, clientKeyUsage)
	flag.StringVar(&ca, caPathParam, ca, caPathUsage)
	flag.StringVar(&serverName, serverNameParam, serverName, serverNameUsage)

	flag.StringVar(&stelaCert, stelaCertParam, stelaCert, stelaCertUsage)
	flag.StringVar(&stelaKey, stelaKeyParam, stelaKey, stelaKeyUsage)
	flag.StringVar(&stelaCA, stelaCAPathParam, stelaCA, stelaCAPathUsage)
	flag.StringVar(&stelaServerName, stelaServerNameParam, stelaServerName, stelaServerNameUsage)

	flag.Parse(os.Args[2:])

	if insecure {
		ca = ""
	}

	if len(addr) == 0 && !noStela {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		var stelaclient *stela_api.Client
		var err error
		if insecure {
			stelaclient, err = stela_api.NewClient(ctx, stela.DefaultStelaAddress, nil)
		} else {
			stelaclient, err = stela_api.NewTLSClient(ctx, stela.DefaultStelaAddress, stelaServerName, stelaCert, stelaKey, stelaCA)
		}

		if err == nil {
			defer stelaclient.Close()

			discoverCtx, cancelDiscover := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancelDiscover()
			service, err := stelaclient.DiscoverOne(discoverCtx, iris.DefaultServiceName)
			fmt.Println(err)
			if err == nil {
				addr = service.IPv4Address()
			}
		}
	}

	if len(addr) == 0 {
		addr = fmt.Sprintf("127.0.0.1:%d", iris.DefaultServicePort)
	}
	logger.Info("Connecting", "addr", addr)

	var client *api.Client
	var err error
	connectCtx, cancelConnect := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelConnect()
	if insecure || len(ca) == 0 {
		client, err = api.NewClient(connectCtx, addr, nil)
	} else {
		client, err = api.NewTLSClient(connectCtx, addr, serverName, clientCert, clientKey, ca)
	}

	if err != nil {
		logger.Error("Failed to connect to Iris server", "error", err.Error())
		return exitStatusError
	}
	defer client.Close()

	r := &runner{Client: client, Logger: &logger}
	switch command {
	case setCommandName:
		err = r.setValue(source, key, []byte(value))
	case getCommandName:
		err = r.getValue(source, key)
	case getSourcesCommandName:
		err = r.getSources()
	case getKeysCommandName:
		err = r.getKeys(source)
	case removeSourceCommandName:
		err = r.removeSource(source)
	case removeValueCommandName:
		err = r.removeValue(source, key)
	default:
		err = errors.New("Unknown command")
	}

	if err != nil {
		logger.Error("Command failed", "error", err.Error())
		return exitStatusError
	}

	return exitStatusSuccess
}
