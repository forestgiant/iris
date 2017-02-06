package mapsource

import (
	"sync"

	"gitlab.fg/otis/iris"
)

//MapSource is an implementation of the Source interface built golang's map type
type MapSource struct {
	id      string
	storage map[string][]byte

	initialized  bool
	storageMutex *sync.Mutex
}

//NewMapSource returns a new map source with the desired identifier
func NewMapSource(identifier string) *MapSource {
	return &MapSource{id: identifier}
}

func (m *MapSource) initialize() {
	if m.initialized {
		return
	}

	m.initialized = true
	m.storageMutex = &sync.Mutex{}
}

//ID returns the identifier for this source
func (m *MapSource) ID() string {
	if len(m.id) == 0 {
		return iris.DefaultIdentifier
	}
	return m.id
}

//Set stores the value
func (m *MapSource) Set(key string, value []byte) error {
	m.initialize()

	m.storageMutex.Lock()
	defer m.storageMutex.Unlock()

	if m.storage == nil {
		m.storage = make(map[string][]byte)
	}
	m.storage[key] = value
	return nil
}

//SetKeyValuePair is a helper for Set that accepts a KeyValuePair object
func (m *MapSource) SetKeyValuePair(kvp iris.KeyValuePair) error {
	return m.Set(kvp.Key, kvp.Value)
}

//Get retrieves the stored value
func (m *MapSource) Get(key string) (value []byte, err error) {
	m.initialize()

	m.storageMutex.Lock()
	defer m.storageMutex.Unlock()

	value = m.storage[key]
	return value, nil
}

//GetKeyValuePair retrives the stored value as a KeyValuePair
func (m *MapSource) GetKeyValuePair(key string) (iris.KeyValuePair, error) {
	var kvp = iris.KeyValuePair{
		Key:   key,
		Value: m.storage[key],
	}
	return kvp, nil
}

//GetKeys returns a slice of keys present in storage
func (m *MapSource) GetKeys() ([]string, error) {
	m.initialize()

	m.storageMutex.Lock()
	defer m.storageMutex.Unlock()

	keys := make([]string, 0, len(m.storage))
	for k := range m.storage {
		keys = append(keys, k)
	}
	return keys, nil
}

//Remove removes the pair associated with the specified key
func (m *MapSource) Remove(key string) error {
	m.initialize()

	m.storageMutex.Lock()
	defer m.storageMutex.Unlock()

	delete(m.storage, key)
	return nil
}

//RemoveKeyValuePair removes the specified pair from the source
func (m *MapSource) RemoveKeyValuePair(kvp iris.KeyValuePair) error {
	return m.Remove(kvp.Key)
}
