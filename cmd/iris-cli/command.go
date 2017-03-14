package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/forestgiant/iris/api"
	fglog "github.com/forestgiant/log"
)

type runner struct {
	Client *api.Client
	Logger *fglog.Logger
}

func (r *runner) setValue(source string, key string, value []byte) error {
	if len(source) == 0 {
		return errors.New("You must provide a source")
	}

	if len(key) == 0 {
		return errors.New("You must provide a key")
	}

	if len(value) == 0 {
		return errors.New("You must provide a value")
	}

	commandCtx, cancelCommand := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelCommand()

	if err := r.Client.SetValue(commandCtx, source, key, []byte(value)); err != nil {
		return err
	}

	r.Logger.Info("Success", "source", source, "key", key, "value", string(value))
	return nil
}

func (r *runner) getValue(source, key string) error {
	if len(source) == 0 {
		return errors.New("You must provide a source")
	}

	if len(key) == 0 {
		return errors.New("You must provide a key")
	}

	commandCtx, cancelCommand := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelCommand()

	value, err := r.Client.GetValue(commandCtx, source, key)
	if err != nil {
		return err
	}

	r.Logger.Info("Success", "source", source, "key", key, "value", string(value))
	return nil
}

func (r *runner) getSources() error {
	commandCtx, cancelCommand := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelCommand()

	sources, err := r.Client.GetSources(commandCtx)
	if err != nil {
		return err
	}

	r.Logger.Info("Success", "count", len(sources))
	for _, s := range sources {
		fmt.Println(s)
	}
	return nil
}

func (r *runner) getKeys(source string) error {
	if len(source) == 0 {
		return errors.New("You must provide a source")
	}

	commandCtx, cancelCommand := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelCommand()

	keys, err := r.Client.GetKeys(commandCtx, source)
	if err != nil {
		return err
	}

	r.Logger.Info("Success", "source", source, "count", len(keys))
	for _, k := range keys {
		fmt.Println(k)
	}
	return nil
}

func (r *runner) removeSource(source string) error {
	if len(source) == 0 {
		return errors.New("You must provide a source")
	}

	commandCtx, cancelCommand := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelCommand()

	if err := r.Client.RemoveSource(commandCtx, source); err != nil {
		return err
	}

	r.Logger.Info("Success", "source", source)
	return nil
}

func (r *runner) removeValue(source, key string) error {
	if len(source) == 0 {
		return errors.New("You must provide a source")
	}

	if len(key) == 0 {
		return errors.New("You must provide a key")
	}

	commandCtx, cancelCommand := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelCommand()

	if err := r.Client.RemoveValue(commandCtx, source, key); err != nil {
		return err
	}

	r.Logger.Info("Success", "source", source, "key", key)
	return nil
}
