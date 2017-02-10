package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	fglog "github.com/forestgiant/log"
	"gitlab.fg/go/stela"
	stela_api "gitlab.fg/go/stela/api"
	"gitlab.fg/otis/iris"
	"gitlab.fg/otis/iris/api"
)

const (
	setCommandName          = "set"
	getCommandName          = "get"
	removeSourceCommandName = "removesource"
	removeKeyCommandName    = "removekey"

	promptCommand = "prompt for command"
	promptSource  = "prompt for source"
	promptKey     = "prompt for key"

	sourceUsage = "The name of the source to be used."
	sourceParam = "source"

	keyUsage = "The name of the key to be used."
	keyParam = "key"

	valueUsage = "The value to be used."
	valueParam = "value"

	addrUsage = "Address of the stela server to connect to."
	addrParam = "addr"

	caPathUsage = "Path to the certificate authority you would like to use."
	caPathParam = "ca"

	exitStatusSuccess = 0
	exitStatusError   = 1
)

func main() {
	os.Exit(run())
}

func run() (status int) {

	// Setup a logger to use
	var logger = fglog.Logger{}.With("time", fglog.DefaultTimestamp)

	var (
		ca      string
		addr    string
		command string
		source  string
		key     string
		value   string
	)

	// Prepare default path values
	wd, err := os.Getwd()
	if err != nil {
		logger.Error("unable to get current working directory", "error", err)
		return exitStatusError
	}
	ca = filepath.Join(wd, "ca.cer")

	if len(os.Args) == 1 || os.Args[1] == "-h" || os.Args[1] == "-help" {

		if len(os.Args) == 1 {
			return exitStatusError
		}
		return exitStatusSuccess
	}

	command = os.Args[1]

	if command != setCommandName && command != getCommandName &&
		command != removeSourceCommandName && command != removeKeyCommandName {
		printUsageInstructions()
		return exitStatusError
	}

	flag := flag.NewFlagSet(command, flag.ExitOnError)
	flag.StringVar(&ca, caPathParam, ca, caPathUsage)
	flag.StringVar(&addr, addrParam, addr, addrUsage)
	flag.StringVar(&source, sourceParam, source, sourceUsage)
	flag.StringVar(&key, keyParam, key, keyUsage)
	flag.StringVar(&value, valueParam, value, valueUsage)
	flag.Parse(os.Args[2:])

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	stelaclient, err := stela_api.NewClient(ctx, stela.DefaultStelaAddress, "")
	if err != nil {
		logger.Error("Failed to obtain stela client.", "error", err.Error())
		return exitStatusError
	}

	if len(addr) == 0 {
		discoverCtx, cancelDiscover := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancelDiscover()
		service, err := stelaclient.DiscoverOne(discoverCtx, iris.DefaultServiceName)
		if err != nil {
			logger.Error("Failed to discover iris server.", "error", err.Error())
			return exitStatusError
		}
		addr = service.IPv4Address()
	}

	var client *api.Client
	connectCtx, cancelConnect := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelConnect()
	if len(ca) == 0 {
		client, err = api.NewClient(connectCtx, addr, nil)
	} else {
		client, err = api.NewTLSClient(connectCtx, addr, "iris.forestgiant.com", ca)
	}

	if err != nil {
		logger.Error("Failed to connect to iris server.", "error", err.Error())
		return exitStatusError
	}

	commandCtx, cancelCommand := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelCommand()

	switch command {
	case setCommandName:
		if err := client.SetValue(commandCtx, source, key, []byte(value)); err != nil {
			logger.Error("Failed to set value.", "error", err.Error())
			return exitStatusError
		}
		logger.Info("Success", "command", command, "source", source, "key", key, "value", value)
	case getCommandName:
		value, err := client.GetValue(commandCtx, source, key)
		if err != nil {
			logger.Error("Failed to get value.", "error", err.Error())
			return exitStatusError
		}
		logger.Info("Success", "command", command, "source", source, "key", key, "value", string(value))
	case removeSourceCommandName:
		if err := client.RemoveSource(commandCtx, source); err != nil {
			logger.Error("Failed to remove source.", "error", err.Error())
			return exitStatusError
		}
		logger.Info("Success", "command", command, "source", source, "key")
	case removeKeyCommandName:
		if err := client.RemoveValue(commandCtx, source, key); err != nil {
			logger.Error("Failed to remove source/key/value.", "error", err.Error())
			return exitStatusError
		}
		logger.Info("Success", "command", command, "source", source, "key", key)
	default:
		logger.Error("Unknown command")
		return exitStatusError
	}

	return exitStatusSuccess
}

func printUsageInstructions() {
	fmt.Println("usage: iris-cli <command> [<args>]")
	fmt.Println("The available commands are: ")
	fmt.Printf("\t%s\t\tSet a value\n", setCommandName)
	fmt.Printf("\t%s\t\tGet a value\n", getCommandName)
	fmt.Printf("\t%s\tRemove a source\n", removeSourceCommandName)
	fmt.Printf("\t%s\tRemove a key/value pair\n", removeKeyCommandName)
}
