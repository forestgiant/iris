package store

import (
	"encoding/json"
	"io"

	"github.com/hashicorp/raft"
)

type fsm Store

func (f *fsm) set(source, key string, value []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.storage[source] == nil {
		f.storage[source] = make(kvs)
	}
	f.storage[source][key] = value
}

func (f *fsm) deleteSource(source string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	keys := []string{}
	if m, ok := f.storage[source]; ok {
		for k := range m {
			keys = append(keys, k)
		}
		delete(f.storage, source)
	}

	return keys
}

func (f *fsm) deleteKey(source, key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	var found = false
	if m, ok := f.storage[source]; ok {
		if _, ok := m[key]; ok {
			found = true
			delete(m, key)
		}

		if len(m) == 0 {
			delete(f.storage, source)
		}
	}

	return found
}

func (f *fsm) applyCommand(c command) interface{} {
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

func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		f.logger.Error("Failed to unmarshal command.", "error", err)
		return nil
	}

	return f.applyCommand(c)
}

func (f *fsm) applySet(source string, key string, value []byte) interface{} {
	f.logger.Info("SET", "source", source, "key", key, "value", value)
	f.set(source, key, value)
	go f.publishCallback(source, key, value)

	return nil
}

func (f *fsm) appleDeleteSource(source string) interface{} {
	f.logger.Info("DELETE", "source")
	deletedKeys := f.deleteSource(source)
	for _, k := range deletedKeys {
		go f.publishCallback(source, k, nil)
	}
	return nil
}

func (f *fsm) appleDeleteKey(source string, key string) interface{} {
	f.logger.Info("DELETE", "source", source, "key", key)
	if f.deleteKey(source, key) {
		go f.publishCallback(source, key, nil)
	}
	return nil
}

func clone(o map[string]kvs) map[string]kvs {
	clone := make(map[string]kvs)
	for s, m := range o {
		clone[s] = make(kvs)
		for k, v := range m {
			clone[s][k] = v
		}
	}
	return clone
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &fsmSnapshot{store: clone(f.storage)}, nil
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
