package store

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"encoding/json"

	fglog "github.com/forestgiant/log"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

const (
	operationSet          = "set"
	operationDeleteKey    = "deletekey"
	operationDeleteSource = "deleteSource"
)

type command struct {
	Operation string `json:"operation, omitempty"`
	Source    string `json:"string, omitempty"`
	Key       string `json:"key, omitempty"`
	Value     []byte `json:"value, omitempty"`
}

type kvs map[string][]byte

// Store is a collection of key-value stores, where all changes are made via Raft consensus
type Store struct {
	RaftBindAddr string
	RaftDir      string

	raft   *raft.Raft
	logger *fglog.Logger

	mu      sync.Mutex
	storage map[string]kvs
}

// NewStore initializes a new store with the provided properties
func NewStore(raftBindAddr, raftDir string, logger fglog.Logger) *Store {
	return &Store{
		RaftBindAddr: raftBindAddr,
		RaftDir:      raftDir,
		storage:      make(map[string]kvs),
		logger:       &logger,
	}
}

// Open the store.  If enableSingle is set, and there are no existing peers, this first node becomes the leader of the cluster
func (s *Store) Open(enableSingle bool) error {
	// Setup raft configuration
	config := raft.DefaultConfig()

	// Setup raft communication
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBindAddr)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(s.RaftBindAddr, addr, 3, raftTimeout, os.Stdout)
	if err != nil {
		return err
	}

	// Create the peer store
	peerStore := raft.NewJSONPeers(s.RaftDir, transport)

	// Get any existing peers
	peers, err := peerStore.Peers()
	if err != nil {
		return err
	}

	// Enable single mode if the option is set and this is the first node
	if enableSingle && len(peers) == 0 {
		s.logger.Info("Enabling single mode.")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// Create the snapshot store. This allows raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stdout)
	if err != nil {
		return err
	}

	// Create the boltdb store (log and stable stores)
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return err
	}

	// Setup the raft consensus mechanism
	r, err := raft.NewRaft(config, (*fsm)(s), boltStore, boltStore, snapshots, peerStore, transport)
	if err != nil {
		return err
	}

	s.raft = r
	return nil
}

// Set the value for the given source and key in storage
func (s *Store) Set(source string, key string, value []byte) error {
	if s.raft.State() != raft.Leader {
		return errors.New("Set should be only called on the leader")
	}

	c := &command{Operation: operationSet, Source: source, Key: key, Value: value}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	return s.raft.Apply(b, raftTimeout).Error()
}

// GetSources returns a list of sources found in storage
func (s *Store) GetSources() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var response = []string{}
	for k := range s.storage {
		response = append(response, k)
	}
	return response, nil
}

// GetKeys returns a list of keys for the given source found in storage
func (s *Store) GetKeys(source string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var response = []string{}
	for k := range s.storage[source] {
		response = append(response, k)
	}
	return response, nil
}

// Get the value for the given source and key in storage
func (s *Store) Get(source string, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.storage[source] == nil {
		return nil, nil
	}
	return s.storage[source][key], nil
}

// DeleteKey deletes the key and value for the given source in storage
func (s *Store) DeleteKey(source string, key string) error {
	if s.raft.State() != raft.Leader {
		return errors.New("DeleteKey should be only called on the leader")
	}

	c := &command{Operation: operationDeleteKey, Source: source, Key: key}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return s.raft.Apply(b, raftTimeout).Error()
}

// DeleteSource deletes the given source in storage
func (s *Store) DeleteSource(source string) error {
	if s.raft.State() != raft.Leader {
		return errors.New("DeleteSource should be only called on the leader")
	}

	c := &command{Operation: operationDeleteSource, Source: source}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return s.raft.Apply(b, raftTimeout).Error()
}

// Join the node located at addr to this store.
// The node must be ready to respond to raft communications
func (s *Store) Join(addr string) error {
	s.logger.Info("Received join request for remote node", "address", addr)
	f := s.raft.AddPeer(addr)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Info("Node successfully joined", "address", addr)
	return nil
}
