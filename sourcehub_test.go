package sourcehub

import (
	"encoding/json"
	"errors"
	"testing"
)

type BadMarshaller struct{}

func (b BadMarshaller) Marshal(object interface{}) ([]byte, error) {
	return nil, errors.New("bad marshaller")
}

func TestJSONMarshaller(t *testing.T) {
	marshaller := JSONMarshaller{}
	kvp := &KeyValuePair{Key: "testkey", Value: []byte("testvalue")}
	m1, err := marshaller.Marshal(kvp)
	if err != nil {
		t.Fatal(err)
	}

	m2, err := json.Marshal(kvp)
	if err != nil {
		t.Fatal(err)
	}

	if len(m1) != len(m2) {
		t.Fatal("marshalled results did not match expected length")
	}

	for i := range m1 {
		if m1[i] != m2[i] {
			t.Fatal("marshalled results did not match expected values")
		}
	}
}

func TestBadMarshaller(t *testing.T) {
	kvp := KeyValuePair{Key: "k", Value: []byte("v")}
	badmarshaller := BadMarshaller{}
	marshalled, err := badmarshaller.Marshal(kvp)
	if err == nil {
		t.Fatal("bad marshaller should produce an error")
	}

	if len(marshalled) > 0 {
		t.Fatal("bad marshaller should produce a zerl-length byte array")
	}

	kvp.marshaller = BadMarshaller{}
	if len(kvp.String()) > 0 {
		t.Fatal("bad KeyValuePair.Marshaller should produce an empty string")
	}
}

func TestKeyValuePairToString(t *testing.T) {
	kvp := &KeyValuePair{Key: "testkey", Value: []byte("testvalue")}
	marshalled, err := json.Marshal(kvp)
	if err != nil {
		t.Fatal(err)
	}

	if string(marshalled) != kvp.String() {
		t.Fatal("marshalled results did not match expected results")
	}

	marshaller := JSONMarshaller{}
	kvp.marshaller = marshaller
	marshalled, err = marshaller.Marshal(kvp)
	if err != nil {
		t.Fatal(err)
	}

	if string(marshalled) != kvp.String() {
		t.Fatal("marshalled results did not match expected results")
	}
}
