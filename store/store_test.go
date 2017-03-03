package store

import (
	"fmt"
	"testing"

	"os"

	fglog "github.com/forestgiant/log"
	"github.com/forestgiant/portutil"
)

var (
	testStore *Store
)

type SuppressedWriter struct{}

func (w *SuppressedWriter) Write(p []byte) (n int, err error) {
	return 0, nil
}

func TestMain(m *testing.M) {
	run := func() int {
		p, err := portutil.GetUniqueTCP()
		if err != nil {
			fmt.Println("Failed to obtain test port")
			return 1
		}

		raftAddr := fmt.Sprintf("127.0.0.1:%d", p)
		raftDir := "com.forestgiant.iris.testing.store.raftDir"
		logger := fglog.Logger{Writer: &SuppressedWriter{}}

		testStore = NewStore(raftAddr, raftDir, logger)
		defer os.RemoveAll(raftDir)
		if err := testStore.Open(true); err != nil {
			fmt.Println("Failed to open test store.", err)
			return 1
		}

		testStore.mu.Lock()
		testStore.storage["testsource1"] = make(kvs)
		testStore.storage["testsource1"]["testkey1"] = []byte("testvalue1")
		testStore.storage["testsource1"]["testkey2"] = []byte("testvalue2")
		testStore.storage["testsource2"] = make(kvs)
		testStore.storage["testsource2"]["testkey1"] = []byte("testvalue1")
		testStore.storage["testsource2"]["testkey2"] = []byte("testvalue2")
		testStore.mu.Unlock()

		return m.Run()
	}

	os.Exit(run())
}

func TestOpenAsLeader(t *testing.T) {
	if testStore.raft == nil {
		t.Error("Open succeeded but store.raft property is nil")
		return
	}

	if !testStore.IsLeader() {
		t.Error("Store should be the leader of the cluster")
		return
	}

	if len(testStore.Leader()) == 0 {
		t.Error("Leader should return a non-empty address")
		return
	}
}

func TestGetSourcesAndKeys(t *testing.T) {
	var sources []string

	t.Run("TestGetSources", func(t *testing.T) {
		var err error
		sources, err = testStore.GetSources()
		if err != nil {
			t.Error(err)
			return
		}

		if len(sources) != len(testStore.storage) {
			t.Error("Wrong number of sources returned.  Expected", len(testStore.storage), "but returned", len(sources))
			return
		}

		for s := range testStore.storage {
			found := false
			for _, source := range sources {
				if source == s {
					found = true
					break
				}
			}
			if !found {
				t.Error("Source missing from returned list.", s)
				return
			}
		}
	})

	t.Run("TestGetKeys", func(t *testing.T) {
		for _, source := range sources {
			keys, err := testStore.GetKeys(source)
			if err != nil {
				t.Error(err)
				return
			}

			if len(keys) != len(testStore.storage[source]) {
				t.Error("Wrong number of keys returned for source.  Expected", len(testStore.storage[source]), "but returned", len(keys))
				return
			}

			for k := range testStore.storage[source] {
				found := false
				for _, key := range keys {
					if key == k {
						found = true
						break
					}
				}

				if !found {
					t.Error("Key missing from returned list.", k)
					return
				}
			}
		}
	})
}

func TestGet(t *testing.T) {
	t.Run("TestProperValuesReturned", func(t *testing.T) {
		tests := []struct {
			Source string
			Key    string
			Value  []byte
		}{
			{Source: "testsource1", Key: "testkey1", Value: []byte("testvalue1")},
			{Source: "testsource1", Key: "testkey2", Value: []byte("testvalue2")},
			{Source: "testsource2", Key: "testkey1", Value: []byte("testvalue1")},
			{Source: "testsource2", Key: "testkey2", Value: []byte("testvalue2")},
		}

		for _, test := range tests {
			v := testStore.Get(test.Source, test.Key)
			if !valuesMatch(v, test.Value) {
				t.Error("Received value does not match expected value.")
			}
		}
	})

	t.Run("TestUnknownSource", func(t *testing.T) {
		v := testStore.Get("unknownsource", "testkey1")
		if v != nil {
			t.Error("Get should return an empty value for an unknown source.")
		}
	})

	t.Run("TestUnknownKey", func(t *testing.T) {
		v := testStore.Get("testsource1", "unknownkey")
		if v != nil {
			t.Error("Get should return an empty value for an unknown source.")
		}
	})
}

func TestSet(t *testing.T) {
	t.Run("TestNotLeader", func(t *testing.T) {
		notleader := NewStore("", "", fglog.Logger{Writer: &SuppressedWriter{}})
		if notleader.IsLeader() {
			t.Error("Store should not be the leader if Open was never called.")
		}

		if err := notleader.Set("source", "key", []byte("value")); err == nil {
			t.Error("Set should fail if the store is not the leader.")
		}
	})

	t.Run("TestExpectedValue", func(t *testing.T) {
		testSetSource := "testsetsource"
		testSetKey := "testsetkey"
		testSetValue := []byte("testsetvalue")
		err := testStore.Set(testSetSource, testSetKey, testSetValue)
		if err != nil {
			t.Error(err)
		}

		testStore.mu.Lock()
		if testStore.storage == nil {
			t.Error("Underlying storage is still nil")
		}
		if testStore.storage[testSetSource] == nil {
			t.Error("Underlying storage does not have an entry for the source")
		}
		if !valuesMatch(testSetValue, testStore.storage[testSetSource][testSetKey]) {
			t.Error("Value not properly set in underlying storage")
		}
		testStore.mu.Unlock()
	})
}

func TestDeleteKey(t *testing.T) {
	t.Run("TestNotLeader", func(t *testing.T) {
		notleader := NewStore("", "", fglog.Logger{Writer: &SuppressedWriter{}})
		if notleader.IsLeader() {
			t.Error("Store should not be the leader if Open was never called.")
		}

		if err := notleader.DeleteKey("testdeletekeysource", "testdeletekeykey"); err == nil {
			t.Error("DeleteKey should fail if the store is not the leader.")
		}
	})

	t.Run("TestExpectedValue", func(t *testing.T) {
		testDeleteSource := "testdeletekeysource"
		testDeleteKey := "testdeletekeykey"

		testStore.mu.Lock()
		if testStore.storage == nil {
			testStore.storage = make(map[string]kvs)
		}
		if testStore.storage[testDeleteSource] == nil {
			testStore.storage[testDeleteSource] = make(kvs)
		}
		testStore.storage[testDeleteSource][testDeleteKey] = []byte("testdeletekeyvalue")
		testStore.mu.Unlock()

		if err := testStore.DeleteKey(testDeleteSource, testDeleteKey); err != nil {
			t.Error(err)
		}

		testStore.mu.Lock()
		if testStore.storage != nil && testStore.storage[testDeleteSource] != nil && testStore.storage[testDeleteSource][testDeleteKey] != nil {
			t.Error("Value was not removed from underlying storage.")
		}
		testStore.mu.Unlock()
	})
}

func TestDeleteSource(t *testing.T) {
	t.Run("TestNotLeader", func(t *testing.T) {
		notleader := NewStore("", "", fglog.Logger{Writer: &SuppressedWriter{}})
		if notleader.IsLeader() {
			t.Error("Store should not be the leader if Open was never called.")
		}

		if err := notleader.DeleteSource("testdeletesource"); err == nil {
			t.Error("DeleteSource should fail if the store is not the leader.")
		}
	})

	t.Run("TestExpectedValue", func(t *testing.T) {
		testDeleteSource := "testdeletesourcesource"
		testStore.mu.Lock()
		if testStore.storage == nil {
			testStore.storage = make(map[string]kvs)
		}
		if testStore.storage[testDeleteSource] == nil {
			testStore.storage[testDeleteSource] = make(kvs)
		}
		testStore.mu.Unlock()

		if err := testStore.DeleteSource(testDeleteSource); err != nil {
			t.Error(err)
		}

		testStore.mu.Lock()
		if testStore.storage != nil && testStore.storage[testDeleteSource] != nil {
			t.Error("Source was not removed from underlying storage.")
		}
		testStore.mu.Unlock()
	})
}

func TestJoin(t *testing.T) {
	t.Run("TestNotLeader", func(t *testing.T) {
		notleader := NewStore("", "", fglog.Logger{Writer: &SuppressedWriter{}})
		if notleader.IsLeader() {
			t.Error("Store should not be the leader if Open was never called.")
		}

		if err := notleader.Join("testjoinsource"); err == nil {
			t.Error("Join should fail if the store is not the leader.")
		}
	})
}

func valuesMatch(v1 []byte, v2 []byte) bool {
	if len(v1) != len(v2) {
		return false
	}

	for i := range v1 {
		if v1[i] != v2[i] {
			return false
		}
	}

	return true
}
