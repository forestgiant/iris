package store

import (
	"encoding/json"
	"io"

	"github.com/hashicorp/raft"
)

type fsm Store

func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		f.logger.Error("Failed to unmarshal command.", "error", err)
		return nil
	}

	switch c.Operation {
	case operationSet:
		return f.applySet(c.Source, c.Key, c.Value)
	case operationDeleteSource:
		return f.appleDeleteSource(c.Source)
	case operationDeleteKey:
		return f.appleDeleteKey(c.Source, c.Key)
	default:
		f.logger.Error("Unrecognized transaction operation.", "operation", c.Operation)
		return nil
	}
}

func (f *fsm) applySet(source string, key string, value []byte) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.storage[source] == nil {
		f.storage[source] = make(kvs)
	}
	f.storage[source][key] = value
	f.logger.Info("SET", "source", source, "key", key, "value", value)
	go f.publishCallback(source, key, value)

	return nil
}

func (f *fsm) appleDeleteSource(source string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logger.Info("DELETE", "source")
	if m, ok := f.storage[source]; ok {
		for k, v := range m {
			go f.publishCallback(source, k, v)
		}
		delete(f.storage, source)
	}
	return nil
}

func (f *fsm) appleDeleteKey(source string, key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.logger.Info("DELETE", "source", source, "key", key)
	if m, ok := f.storage[source]; ok {
		delete(m, key)
		go f.publishCallback(source, key, []byte{})
		if len(m) == 0 {
			delete(f.storage, source)
		}
	}

	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	clone := make(map[string]kvs)
	for s, m := range f.storage {
		f.storage[s] = make(kvs)
		for k, v := range m {
			clone[s][k] = v
		}
	}

	return &fsmSnapshot{store: clone}, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	s := make(map[string]kvs)
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return err
	}

	// Set the state from the snapshot
	// No lock required according to Hashicorp docs
	f.storage = s
	return nil
}

func (f *fsm) publishCallback(source string, key string, value []byte) {
	if f.PublishCallback != nil {
		go f.PublishCallback(source, key, value)
	}
}

type fsmSnapshot struct {
	store map[string]kvs
}

func (f *fsmSnapshot) Persist(s raft.SnapshotSink) error {
	err := func() error {
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		if _, err := s.Write(b); err != nil {
			return err
		}

		if err := s.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		s.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnapshot) Release() {}
