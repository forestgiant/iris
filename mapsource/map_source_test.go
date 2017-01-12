package mapsource

import (
	"testing"

	"gitlab.fg/otis/sourcehub"
)

func TestMapSource_GetID(t *testing.T) {
	var m = NewMapSource("")
	if m.ID() != sourcehub.DefaultIdentifier {
		t.Errorf("GetID should return the default identifier for a source with no ID provided.")
	}

	var expected = "Identifier"
	m = NewMapSource(expected)
	var result = m.ID()
	if result != expected {
		t.Errorf("GetID did not return the expected identifier.  Expected %s, but returned %s.", expected, result)
	}
}

func TestMapSource_SetGet(t *testing.T) {
	var m = NewMapSource("")
	var key = "TestKey"
	var value = "TestValue"
	var err = m.Set(key, []byte(value))
	if err != nil {
		t.Error(err)
	}

	result, err := m.Get(key)
	if err != nil {
		t.Error(err)
	}

	if string(result) != value {
		t.Errorf("Unexpected value retrieved from MapSource for key.  Expected %s, but received %s.", value, result)
	}
}

func TestMapSource_SetGetKeyValuePair(t *testing.T) {
	var m = new(MapSource)
	var kvp = sourcehub.KeyValuePair{Key: "TestKey", Value: []byte("TestValue")}
	var err = m.SetKeyValuePair(kvp)
	if err != nil {
		t.Error(err)
	}

	var retreived sourcehub.KeyValuePair
	retreived, err = m.GetKeyValuePair(kvp.Key)
	if err != nil {
		t.Error(err)
	}

	if retreived.Key != kvp.Key {
		t.Errorf("Retrieved KeyValuePair has a key of %s, but was expected to have a key of %s.", retreived.Key, kvp.Key)
	}

	var rstring = string(retreived.Value)
	var estring = string(kvp.Value)
	if rstring != estring {
		t.Errorf("Retrieved KeyValuePair has a value of %s, but was expected to have a value of %s.", rstring, estring)
	}

	if retreived.String() != kvp.String() {
		t.Errorf("Retrieved KeyValuePair does not match the provided KeyValuePair.")
	}
}

func TestMapSource_GetKeys(t *testing.T) {
	var m = new(MapSource)
	var keys = []string{"one", "two", "three", "four", "five"}
	for _, k := range keys {
		if err := m.Set(k, []byte("value")); err != nil {
			t.Error(err)
		}
	}

	retreivedKeys, err := m.GetKeys()
	if err != nil {
		t.Error(err)
	}

	if len(retreivedKeys) != len(keys) {
		t.Errorf("Expected to receive an array of %d keys, but received an array of %d keys.", len(keys), len(retreivedKeys))
	}

	for _, k := range keys {
		var keyFound = false
		for _, retreived := range retreivedKeys {
			if retreived == k {
				keyFound = true
				break
			}
		}

		if keyFound == false {
			t.Errorf("Unable to find all expected keys in the retreived list.  Missing the following key: %s.", k)
		}
	}
}
