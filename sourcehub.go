package sourcehub

import "encoding/json"

//DefaultServiceName for the source-hub service
const DefaultServiceName = "sourcehub.service.fg"

//DefaultIdentifier is the default identifier for sources to use in their implementations
var DefaultIdentifier = "default"

//Source is an interface describing the ability to store and retrieve data
type Source interface {
	ID() string
	Set(key string, value []byte) error
	SetKeyValuePair(kvp KeyValuePair) error
	Get(key string) (value []byte, err error)
	GetKeyValuePair(key string) (KeyValuePair, error)
	GetKeys() ([]string, error)
}

// Marshaller comment
type Marshaller interface {
	Marshal(object interface{}) ([]byte, error)
}

// JSONMarshaller comment
type JSONMarshaller struct{}

// Marshal comment
func (j JSONMarshaller) Marshal(object interface{}) ([]byte, error) {
	return json.Marshal(object)
}

//KeyValuePair is a structure for storing named data
type KeyValuePair struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`

	marshaller Marshaller
}

//Marshal this key-value pair into a string representation
func (p KeyValuePair) String() string {
	var f Marshaller = JSONMarshaller{}
	if p.marshaller != nil {
		f = p.marshaller
	}

	bytes, err := f.Marshal(p)
	if err != nil {
		return ""
	}
	return string(bytes)
}
