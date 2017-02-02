package mapsource

import (
	"sync"

	"gitlab.fg/otis/sourcehub"
)

//MapSource is an implementation of the Source interface built golang's map type
type MapSource struct {
	id           string
	storage      map[string][]byte
	storageMutex *sync.Mutex
}

//NewMapSource returns a new map source with the desired identifier
func NewMapSource(identifier string) *MapSource {
	return &MapSource{id: identifier}
}

func (m *MapSource) getStorageMutex() *sync.Mutex {
	if m.storageMutex == nil {
		m.storageMutex = &sync.Mutex{}
	}
	return m.storageMutex
}

//ID returns the identifier for this source
func (m *MapSource) ID() string {
	if len(m.id) == 0 {
		return sourcehub.DefaultIdentifier
	}
	return m.id
}

//Set stores the value
func (m *MapSource) Set(key string, value []byte) error {
	mutex := m.getStorageMutex()
	mutex.Lock()
	defer mutex.Unlock()

	if m.storage == nil {
		m.storage = make(map[string][]byte)
	}
	m.storage[key] = value
	return nil
}

//SetKeyValuePair is a helper for Set that accepts a KeyValuePair object
func (m *MapSource) SetKeyValuePair(kvp sourcehub.KeyValuePair) error {
	m.Set(kvp.Key, kvp.Value)
	return nil
}

//Get retrieves the stored value
func (m *MapSource) Get(key string) (value []byte, err error) {
	mutex := m.getStorageMutex()
	mutex.Lock()
	defer mutex.Unlock()

	value = m.storage[key]
	return value, nil
}

//GetKeyValuePair retrives the stored value as a KeyValuePair
func (m *MapSource) GetKeyValuePair(key string) (sourcehub.KeyValuePair, error) {
	var kvp = sourcehub.KeyValuePair{
		Key:   key,
		Value: m.storage[key],
	}
	return kvp, nil
}

//GetKeys returns a slice of keys present in storage
func (m *MapSource) GetKeys() ([]string, error) {
	mutex := m.getStorageMutex()
	mutex.Lock()
	defer mutex.Unlock()

	keys := make([]string, 0, len(m.storage))
	for k := range m.storage {
		keys = append(keys, k)
	}
	return keys, nil
}

//Remove removes the pair associated with the specified key
func (m *MapSource) Remove(key string) error {
	mutex := m.getStorageMutex()
	mutex.Lock()
	defer mutex.Unlock()

	delete(m.storage, key)
	return nil
}

//RemoveKeyValuePair removes the specified pair from the source
func (m *MapSource) RemoveKeyValuePair(kvp sourcehub.KeyValuePair) error {
	mutex := m.getStorageMutex()
	mutex.Lock()
	defer mutex.Unlock()

	delete(m.storage, kvp.Key)
	return nil
}
