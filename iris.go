package iris

import "encoding/json"

const (
	//DefaultServicePort for the iris service
	DefaultServicePort = 32000

	//DefaultServiceName for the iris service
	DefaultServiceName = "iris.service.fg"

	//DefaultServerName represents the expected common name for this server
	DefaultServerName = "Iris"

	//DefaultIdentifier is the default identifier for sources to use in their implementations
	DefaultIdentifier = "default"
)

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
