package store

import (
	"testing"

	fglog "github.com/forestgiant/log"
)

func keysMatch(keys1 []string, keys2 []string) bool {
	if len(keys1) != len(keys2) {
		return false
	}

	for _, key1 := range keys1 {
		found := false
		for _, key2 := range keys2 {
			if key1 == key2 {
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

func TestFSM(t *testing.T) {
	s := NewStore("", "", fglog.Logger{Writer: &SuppressedWriter{}})
	fsm := (*fsm)(s)

	t.Run("TestSet", func(t *testing.T) {
		testSource := "testFSMSetSource"
		testKey := "testFSMSetKey"
		testValue := []byte("testFSMSetValue")
		fsm.set(testSource, testKey, testValue)

		fsm.mu.Lock()
		if fsm.storage == nil || fsm.storage[testSource] == nil ||
			!valuesMatch(testValue, fsm.storage[testSource][testKey]) {
			t.Error("FSM set did not result in the appropriate value in storage")
		}
		fsm.mu.Unlock()
	})

	t.Run("TestDeleteSourceWithKeys", func(t *testing.T) {
		testSource := "testFSMDeleteSource"
		testKey1 := "testFSMDeleteSourceKey1"
		testKey2 := "testFSMDeleteSourceKey2"
		testValue := []byte("testFSMDeleteSourceValue")

		fsm.mu.Lock()
		fsm.storage = make(map[string]kvs)
		fsm.storage[testSource] = make(kvs)
		fsm.storage[testSource][testKey1] = testValue
		fsm.storage[testSource][testKey2] = testValue
		fsm.mu.Unlock()

		expected := []string{testKey1, testKey2}
		received := fsm.deleteSource(testSource)
		if !keysMatch(expected, received) {
			t.Error("Keys received from deleteSource did not match the expected set of keys.")
		}

		fsm.mu.Lock()
		if fsm.storage != nil && fsm.storage[testSource] != nil {
			t.Error("Source was not successfully deleted")
		}
		fsm.mu.Unlock()
	})

	t.Run("TestDeleteEmptySource", func(t *testing.T) {
		testSource := "testFSMDeleteSource"
		fsm.mu.Lock()
		fsm.storage = make(map[string]kvs)
		fsm.storage[testSource] = make(kvs)
		fsm.mu.Unlock()

		if len(fsm.deleteSource(testSource)) > 0 {
			t.Error("DeleteSource should have returned an empty set of keys that were deleted.")
		}
	})

	t.Run("TestDeleteUnknownSource", func(t *testing.T) {
		if len(fsm.deleteSource("unknownSource")) > 0 {
			t.Error("DeleteSource should have returned an empty set of keys that were deleted.")
		}
	})

	t.Run("TestDeleteKey", func(t *testing.T) {
		testSource := "testFSMDeleteSource"
		testKey := "testFSMDeleteSourceKey"
		testValue := []byte("testFSMDeleteSourceValue")
		fsm.mu.Lock()
		fsm.storage = make(map[string]kvs)
		fsm.storage[testSource] = make(kvs)
		fsm.storage[testSource][testKey] = testValue
		fsm.mu.Unlock()

		if !fsm.deleteKey(testSource, testKey) {
			t.Error("DeleteKey should have indicated that it successfully removed a key")
			return
		}

		fsm.mu.Lock()
		if fsm.storage != nil && fsm.storage[testSource] != nil && fsm.storage[testSource][testKey] != nil {
			t.Error("DeleteKey did not successfully remove the key")
		}
		fsm.mu.Unlock()
	})

	t.Run("TestDeleteUnknownKey", func(t *testing.T) {
		testSource := "testFSMDeleteSource"
		fsm.mu.Lock()
		fsm.storage = make(map[string]kvs)
		fsm.storage[testSource] = make(kvs)
		fsm.mu.Unlock()

		if fsm.deleteKey(testSource, "unknownKey") {
			t.Error("DeleteKey should have indicated that key removal was not required")
			return
		}
	})

	t.Run("TestApplyBadCommand", func(t *testing.T) {
		c := command{Operation: "testFSMBadCommand"}
		if fsm.applyCommand(c) != nil {
			t.Error("Expected applyCommand to return nil")
		}
	})

	t.Run("TestCloneStorage", func(t *testing.T) {
		original := make(map[string]kvs)
		original["cloneSource1"] = make(kvs)
		original["cloneSource1"]["cloneKey1"] = []byte("cloneValue1")
		original["cloneSource1"]["cloneKey2"] = []byte("cloneValue2")
		original["cloneSource1"]["cloneKey3"] = []byte("cloneValue3")
		original["cloneSource2"] = make(kvs)
		original["cloneSource2"]["cloneKey1"] = []byte("cloneValue1")
		original["cloneSource2"]["cloneKey2"] = []byte("cloneValue2")
		original["cloneSource3"] = make(kvs)
		original["cloneSource3"]["cloneKey1"] = []byte("cloneValue1")
		original["cloneSource4"] = make(kvs)

		c := clone(original)
		if c == nil {
			t.Error("Clone should not be nil")
			return
		}

		if len(c) != len(original) {
			t.Error("Number of keys does not match in clone")
			return
		}

		for s, kvs := range c {
			if len(kvs) != len(original[s]) {
				t.Error("Cloned source does not have the same number of keys as the original")
				continue
			}

			for k, v := range kvs {
				if original[s] == nil {
					t.Error("Source should not have been present in clone")
					continue
				}

				if !valuesMatch(original[s][k], v) {
					t.Error("Value in clone did not match original")
					continue
				}
			}
		}
	})
}
