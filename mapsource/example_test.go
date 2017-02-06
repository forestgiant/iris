package mapsource

import (
	"fmt"

	iris "gitlab.fg/otis/iris"
)

func ExampleNewMapSource() {
	source := NewMapSource("sourceIdentifier")
	fmt.Println(source.ID())
	//Output: sourceIdentifier
}

func ExampleMapSource() {
	source := NewMapSource("sourceIdentifier")
	fmt.Println(source.ID())
	//Output: sourceIdentifier
}

func ExampleMapSource_DefaultID() {
	source := MapSource{}
	fmt.Println(source.ID())
	//Output: default
}

func ExampleMapSource_Set() {
	source := NewMapSource("sourceIdentifier")
	key := "key"
	value := []byte("value")

	source.Set(key, value)
	var byteValue, err = source.Get(key)
	if err != nil {
		fmt.Println("Error setting value for key.")
	}
	fmt.Println(string(byteValue))
	//Output: value
}

func ExampleMapSource_SetKeyValuePair() {
	source := NewMapSource("sourceIdentifier")
	pair := iris.KeyValuePair{Key: "key", Value: []byte("value")}
	source.SetKeyValuePair(pair)
	retrieved, err := source.GetKeyValuePair(pair.Key)
	if err != nil {
		fmt.Println("Error retrieving key-value pair for key.")
	}
	fmt.Println(retrieved.Key, string(retrieved.Value))
	//Output:key value
}

func ExampleMapSource_Get() {
	source := NewMapSource("sourceIdentifier")
	key := "key"
	value := []byte("value")

	source.Set(key, value)
	var byteValue, err = source.Get(key)
	if err != nil {
		fmt.Println("Error setting value for key.")
	}
	fmt.Println(string(byteValue))
	//Output: value
}

func ExampleMapSource_GetKeyValuePair() {
	source := NewMapSource("sourceIdentifier")
	pair := iris.KeyValuePair{Key: "key", Value: []byte("value")}
	source.SetKeyValuePair(pair)
	retrieved, err := source.GetKeyValuePair(pair.Key)
	if err != nil {
		fmt.Println("Error retrieving key-value pair for key.")
	}
	fmt.Println(retrieved.Key, string(retrieved.Value))
	//Output:key value
}

func ExampleMapSource_GetKeys() {
	source := NewMapSource("sourceIdentifier")
	source.Set("color", []byte("red"))
	source.Set("time", []byte("now"))
	source.Set("size", []byte("medium"))

	keys, err := source.GetKeys()
	if err != nil {
		fmt.Println("Error getting keys for source.")
	}

	fmt.Println(len(keys))
	//Output:3
}