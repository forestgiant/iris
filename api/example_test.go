package api

import (
	"context"
	"fmt"
	"time"

	"gitlab.fg/otis/iris/pb"
)

func ExampleNewClient() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	testClient, err := NewClient(ctx, "127.0.0.1:32000", nil)
	if err != nil {
		//handle connection error
		return
	}
	defer testClient.Close()
}

func ExampleNewTLSClient() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	testClient, err := NewTLSClient(ctx, "127.0.0.1:32000", "iris.forestgiant.com", "/path/to/certificate-authority.cer")
	if err != nil {
		//handle connection error
		return
	}
	defer testClient.Close()
}

func ExampleClose() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	testClient, err := NewClient(ctx, "127.0.0.1:32000", nil)
	if err != nil {
		//handle connection error
		return
	}
	defer testClient.Close()
}

func ExampleJoin() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	leaderAddress := "127.0.0.1:32000"
	testClient, err := NewClient(ctx, leaderAddress, nil)
	if err != nil {
		//handle connection error
		return
	}
	defer testClient.Close()

	newNodeAddress := "127.0.0.1:32010"
	if err := testClient.Join(ctx, newNodeAddress); err != nil {
		//handle Join error
	}
}

func ExampleGetSources() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	testClient, err := NewClient(ctx, "127.0.0.1:32000", nil)
	if err != nil {
		//handle connection error
		return
	}
	defer testClient.Close()

	sources, err := testClient.GetSources(ctx)
	if err != nil {
		//handle GetSources error
		return
	}

	fmt.Println("The server has stored", len(sources), "sources")
}

func ExampleGetKeys() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	testClient, err := NewClient(ctx, "127.0.0.1:32000", nil)
	if err != nil {
		//handle connection error
		return
	}
	defer testClient.Close()

	keys, err := testClient.GetKeys(ctx, "source")
	if err != nil {
		//handle GetKeys error
		return
	}

	fmt.Println("The server returned", len(keys), "keys for the provided source")
}

func ExampleSetValue() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	testClient, err := NewClient(ctx, "127.0.0.1:32000", nil)
	if err != nil {
		//handle connection error
		return
	}
	defer testClient.Close()

	if err := testClient.SetValue(ctx, "source", "key", []byte("value")); err != nil {
		//handle SetValue error
		return
	}
}

func ExampleGetValue() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	testClient, err := NewClient(ctx, "127.0.0.1:32000", nil)
	if err != nil {
		//handle connection error
		return
	}
	defer testClient.Close()

	value, err := testClient.GetValue(ctx, "source", "key")
	if err != nil {
		//handle GetValue error
		return
	}

	fmt.Println("GetValue returned a value of", value, "for the provided source and key.")
}

func ExampleRemoveValue() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	testClient, err := NewClient(ctx, "127.0.0.1:32000", nil)
	if err != nil {
		//handle connection error
		return
	}
	defer testClient.Close()

	if err := testClient.RemoveValue(ctx, "source", "key"); err != nil {
		//handle RemoveValue error
		return
	}
}

func ExampleRemoveSource() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	testClient, err := NewClient(ctx, "127.0.0.1:32000", nil)
	if err != nil {
		//handle connection error
		return
	}
	defer testClient.Close()

	if err := testClient.RemoveSource(ctx, "source"); err != nil {
		//handle RemoveSource error
		return
	}
}

func ExampleSubscribe() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	testClient, err := NewClient(ctx, "127.0.0.1:32000", nil)
	if err != nil {
		//handle connection error
		return
	}
	defer testClient.Close()

	var handler UpdateHandler = func(u *pb.Update) {
		//this will be called when a value is updated for the source
		fmt.Println("Received updated value", u.Value, "for source", u.Source, "and key", u.Key)
	}

	if _, err := testClient.Subscribe(ctx, "source", &handler); err != nil {
		//handle Subscribe error
	}
}

func ExampleSubscribeKey() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	testClient, err := NewClient(ctx, "127.0.0.1:32000", nil)
	if err != nil {
		//handle connection error
		return
	}
	defer testClient.Close()

	var handler UpdateHandler = func(u *pb.Update) {
		//this will be called when a value is updated for the key
		fmt.Println("Received updated value", u.Value, "for source", u.Source, "and key", u.Key)
	}

	if _, err := testClient.SubscribeKey(ctx, "source", "key", &handler); err != nil {
		//handle SubscribeKey error
	}
}

func ExampleUnsubscribe() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	testClient, err := NewClient(ctx, "127.0.0.1:32000", nil)
	if err != nil {
		//handle connection error
		return
	}
	defer testClient.Close()

	var handler UpdateHandler = func(u *pb.Update) {
		//this will be called when a value is updated for the source
		fmt.Println("Received updated value", u.Value, "for source", u.Source, "and key", u.Key)
	}

	if _, err := testClient.Unsubscribe(ctx, "source", &handler); err != nil {
		//handle Unsubscribe error
	}
}

func ExampleUnsubscribeKey() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	testClient, err := NewClient(ctx, "127.0.0.1:32000", nil)
	if err != nil {
		//handle connection error
		return
	}
	defer testClient.Close()

	var handler UpdateHandler = func(u *pb.Update) {
		//this will be called when a value is updated for the key
		fmt.Println("Received updated value", u.Value, "for source", u.Source, "and key", u.Key)
	}

	if _, err := testClient.UnsubscribeKey(ctx, "source", "key", &handler); err != nil {
		//handle UnsubscribeKey error
	}
}
