package api_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/forestgiant/portutil"
	"google.golang.org/grpc"

	"github.com/forestgiant/iris"
	"github.com/forestgiant/iris/api"
	"github.com/forestgiant/iris/pb"
	"github.com/forestgiant/iris/store"
	"github.com/forestgiant/iris/transport"
	fglog "github.com/forestgiant/log"
)

var testClient *api.Client
var testServiceAddress string

const testColorsSource = "com.forestgiant.iris.testing.colors"
const testSoundsSource = "com.forestgiant.iris.testing.sounds"

const exitStatusSuccess = 0
const exitStatusError = 1

type SuppressedWriter struct{}

func (w *SuppressedWriter) Write(p []byte) (n int, err error) {
	return 0, nil
}

func TestMain(m *testing.M) {
	main := func() int {
		wd, err := os.Getwd()
		if err != nil {
			fmt.Println("Unable to get current working directory", err)
			return exitStatusError
		}

		port, err := portutil.GetUniqueTCP()
		if err != nil {
			fmt.Println("unable to obtain open port", err)
			return exitStatusError
		}

		testServiceAddress = fmt.Sprintf("127.0.0.1:%d", port)
		testRaftAddress := fmt.Sprintf("127.0.0.1:%d", port+1)

		listener, err := net.Listen("tcp", testServiceAddress)
		if err != nil {
			fmt.Println("unable to start tcp listener", err)
			return exitStatusError
		}

		testRaftDir := filepath.Join(wd, "com.forestgiant.iris.testing.client.raftDir")
		defer os.RemoveAll(testRaftDir)

		var store = store.NewStore(testRaftAddress, testRaftDir, fglog.Logger{Writer: &SuppressedWriter{}})
		if err := store.Open(true); err != nil {
			return exitStatusError
		}

		var opts []grpc.ServerOption
		grpcServer := grpc.NewServer(opts...)
		pb.RegisterIrisServer(grpcServer, &transport.Server{
			Store: store,
		})
		errchan := make(chan error)
		go func() {
			errchan <- grpcServer.Serve(listener)
		}()

		statusChan := make(chan int)
		go func() {
			clientCtx, cancelClient := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancelClient()
			testClient, err = api.NewClient(clientCtx, testServiceAddress, nil)
			if err != nil {
				errchan <- err
				return
			}
			statusChan <- m.Run()

		}()

		for {
			select {
			case err := <-errchan:
				fmt.Println(err)
				return exitStatusError
			case status := <-statusChan:
				if err := testClient.Close(); err != nil {
					fmt.Println(err)
				}

				return status
			}
		}
	}

	os.Exit(main())
}

func deleteTestSources() []error {
	var returnErrors []error
	for _, source := range []string{testColorsSource, testSoundsSource} {
		ctx, cancel := context.WithCancel(context.Background())
		if err := testClient.RemoveSource(ctx, source); err != nil {
			returnErrors = append(returnErrors, err)
		}
		cancel()
	}
	return returnErrors
}

func TestNewClient(t *testing.T) {
	tests := []struct {
		Address     string
		ShouldError bool
	}{
		{Address: "", ShouldError: true},
		{Address: "invalid address", ShouldError: true},
		{Address: testServiceAddress, ShouldError: false},
	}

	for _, test := range tests {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c, err := api.NewClient(ctx, test.Address, nil)
		if c != nil {
			if err := c.Close(); err != nil {
				t.Error(err)
				return
			}
		}

		if test.ShouldError && err == nil {
			t.Error("NewClient should produce an error when provided an invalid server address")
			return
		} else if !test.ShouldError && err != nil {
			t.Error("NewClient should not have produced error.", err)
			return
		}
	}
}

func TestSettersAndGetters(t *testing.T) {
	deleteTestSources()

	tests := []struct {
		Source string
		Key    string
		Value  []byte
	}{
		{Source: testColorsSource, Key: "primary", Value: []byte("red")},
		{Source: testColorsSource, Key: "secondary", Value: []byte("green")},
		{Source: testColorsSource, Key: "primary", Value: []byte("blue")},
	}

	expected := make(map[string]map[string][]byte)

	t.Run("TestSetGetValues", func(t *testing.T) {
		for _, test := range tests {
			setContext, cancelSet := context.WithCancel(context.Background())
			defer cancelSet()
			if err := testClient.SetValue(setContext, test.Source, test.Key, test.Value); err != nil {
				t.Errorf("Error setting value. %s", err)
				return
			}

			if expected[test.Source] == nil {
				expected[test.Source] = make(map[string][]byte)
			}

			expected[test.Source][test.Key] = test.Value

			getContext, cancelGet := context.WithCancel(context.Background())
			defer cancelGet()
			value, err := testClient.GetValue(getContext, test.Source, test.Key)
			if err != nil {
				t.Errorf("Error getting value. %s", err)
				return
			}

			if len(test.Value) != len(value) {
				t.Errorf("Error getting value. Received values does not match sent value in length")
				return
			}

			for i := range test.Value {
				if test.Value[i] != value[i] {
					t.Errorf("Error getting value. Received values does not match sent value")
					return
				}
			}
		}
	})

	t.Run("TestGetSources", func(t *testing.T) {
		getSourcesContext, cancelGetSources := context.WithCancel(context.Background())
		defer cancelGetSources()
		sources, err := testClient.GetSources(getSourcesContext)
		if err != nil {
			t.Error("Failed to get sources.", err)
			return
		}

		for _, s := range sources {
			if _, ok := expected[s]; !ok {
				t.Errorf("Unexpected value returned in sources.  Received %s.", s)
				return
			}
		}
	})

	t.Run("TestGetKeys", func(t *testing.T) {
		for source := range expected {
			getKeysContext, cancelGetKeys := context.WithCancel(context.Background())
			defer cancelGetKeys()
			keys, err := testClient.GetKeys(getKeysContext, source)
			if err != nil {
				t.Error("Failed to get keys for source:", source)
				return
			}

			for _, key := range keys {
				if _, ok := expected[source][key]; !ok {
					t.Error("GetKeys returned unexpected key", key)
					return
				}
			}
		}

	})
}

func TestSubscriptions(t *testing.T) {
	t.Run("TestSourceSubscriptions", func(t *testing.T) {
		deleteTestSources()

		resume := make(chan struct{})
		done := make(chan struct{})
		wg := &sync.WaitGroup{}

		unsubscribed := false
		var sourceSubCallback api.UpdateHandler = func(u *pb.Update) {
			if u.Source != testColorsSource {
				t.Error("Received update for ", u.Source, "source, but should only receive updates for the", testColorsSource, "source")
			} else {
				if unsubscribed {
					t.Error("Received update when the client should have been unsubscribed.")
				} else {
					wg.Done()
				}
			}
		}

		var otherCallback api.UpdateHandler = func(u *pb.Update) {
			if u.Source != testColorsSource {
				t.Error("Received update for ", u.Source, "source, but should only receive updates for the", testColorsSource, "source")
			} else {
				wg.Done()
			}
		}

		tests := []struct {
			Source string
			Key    string
			Value  []byte
		}{
			{Source: testColorsSource, Key: "primary", Value: []byte("red")},
			{Source: testColorsSource, Key: "secondary", Value: []byte("green")},
			{Source: testColorsSource, Key: "primary", Value: []byte("blue")},
			{Source: testSoundsSource, Key: "loud", Value: []byte("thunder")},
			{Source: testSoundsSource, Key: "quiet", Value: []byte("snow")},
		}

		sourceSubCtx, cancelSourceSub := context.WithCancel(context.Background())
		defer cancelSourceSub()
		_, err := testClient.Subscribe(sourceSubCtx, testColorsSource, &sourceSubCallback)
		if err != nil {
			t.Error(err)
			return
		}

		_, err = testClient.Subscribe(sourceSubCtx, testColorsSource, &otherCallback)
		if err != nil {
			t.Error(err)
			return
		}

		go func() {
			for _, test := range tests {
				setCtx, cancelSet := context.WithCancel(context.Background())
				defer cancelSet()

				if test.Source == testColorsSource {
					wg.Add(2)
				}
				if err := testClient.SetValue(setCtx, test.Source, test.Key, test.Value); err != nil {
					t.Error("Failed to set test value")
					return
				}
			}

			wg.Wait()
			close(resume)
		}()

		select {
		case <-resume:
		case <-time.After(5 * time.Second):
			t.Error("Subscriptions took too long to respond before unsubscribe.")
			return
		}

		sourceUnsubContext, cancelUnsubSource := context.WithCancel(context.Background())
		defer cancelUnsubSource()
		_, err = testClient.Unsubscribe(sourceUnsubContext, testColorsSource, &sourceSubCallback)
		if err != nil {
			t.Error(err)
			return
		}
		unsubscribed = true

		go func() {
			for _, test := range tests {
				setCtx, cancelSet := context.WithCancel(context.Background())
				defer cancelSet()

				if test.Source == testColorsSource {
					wg.Add(1)
				}
				if err := testClient.SetValue(setCtx, test.Source, test.Key, test.Value); err != nil {
					t.Error("Failed to set test value")
					return
				}
			}

			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Subscriptions took too long to respond after unsubscribe.")
		}

		otherUnsubContext, cancelUnsubOther := context.WithCancel(context.Background())
		defer cancelUnsubOther()
		_, err = testClient.Unsubscribe(otherUnsubContext, testColorsSource, &otherCallback)
		if err != nil {
			t.Error(err)
			return
		}
	})

	t.Run("TestKeySubscriptions", func(t *testing.T) {
		deleteTestSources()

		testKey := "primary"
		tests := []struct {
			Source string
			Key    string
			Value  []byte
		}{
			{Source: testColorsSource, Key: testKey, Value: []byte("red")},
			{Source: testColorsSource, Key: "secondary", Value: []byte("green")},
			{Source: testColorsSource, Key: testKey, Value: []byte("blue")},
			{Source: testSoundsSource, Key: "loud", Value: []byte("thunder")},
			{Source: testSoundsSource, Key: "quiet", Value: []byte("snow")},
		}

		wg := &sync.WaitGroup{}
		resume := make(chan struct{})
		done := make(chan struct{})
		unsubscribed := false
		var keySubCallback api.UpdateHandler = func(u *pb.Update) {
			if u.Source == testColorsSource && u.Key == testKey {
				if unsubscribed {
					t.Error("Received update when the client should have been unsubscribed.")
				} else {
					wg.Done()
				}
			} else {
				t.Error("Received update for ", u, "source, but should only receive updates for the", testColorsSource, "source and", testKey, "key")
			}
		}

		var otherSubCallback api.UpdateHandler = func(u *pb.Update) {
			if u.Source == testColorsSource && u.Key == testKey {
				wg.Done()
			} else {
				t.Error("Received update for ", u, "source, but should only receive updates for the", testColorsSource, "source and", testKey, "key")
			}
		}

		keySubCtx, cancelKeySub := context.WithCancel(context.Background())
		defer cancelKeySub()
		_, err := testClient.SubscribeKey(keySubCtx, testColorsSource, testKey, &keySubCallback)
		if err != nil {
			t.Error(err)
			return
		}

		_, err = testClient.SubscribeKey(keySubCtx, testColorsSource, testKey, &otherSubCallback)
		if err != nil {
			t.Error(err)
			return
		}

		go func() {
			for _, test := range tests {
				setCtx, cancelSet := context.WithCancel(context.Background())
				defer cancelSet()

				if test.Source == testColorsSource && test.Key == testKey {
					wg.Add(2)
				}

				if err := testClient.SetValue(setCtx, test.Source, test.Key, test.Value); err != nil {
					t.Error("Failed to set test value")
					return
				}
			}

			wg.Wait()
			close(resume)
		}()

		select {
		case <-resume:
		case <-time.After(5 * time.Second):
			t.Error("Subscriptions took too long to respond before unsubscribe.")
			return
		}

		keyUnsubContext, cancelUnsubSource := context.WithCancel(context.Background())
		defer cancelUnsubSource()
		_, err = testClient.UnsubscribeKey(keyUnsubContext, testColorsSource, testKey, &keySubCallback)
		if err != nil {
			t.Error(err)
			return
		}
		unsubscribed = true

		go func() {
			for _, test := range tests {
				setCtx, cancelSet := context.WithCancel(context.Background())
				defer cancelSet()

				if test.Source == testColorsSource && test.Key == testKey {
					wg.Add(1)
				}

				if err := testClient.SetValue(setCtx, test.Source, test.Key, test.Value); err != nil {
					t.Error("Failed to set test value")
					return
				}
			}

			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Subscriptions took too long to respond after unsubscribe.")
			return
		}

		otherUnsubContext, cancelUnsubOther := context.WithCancel(context.Background())
		defer cancelUnsubOther()
		_, err = testClient.UnsubscribeKey(otherUnsubContext, testColorsSource, testKey, &otherSubCallback)
		if err != nil {
			t.Error(err)
			return
		}
	})
}

func TestRemoveSource(t *testing.T) {
	deleteTestSources()

	test := iris.KeyValuePair{Key: "primary", Value: []byte("red")}
	setContext, cancelSet := context.WithCancel(context.Background())
	defer cancelSet()
	if err := testClient.SetValue(setContext, testColorsSource, test.Key, test.Value); err != nil {
		t.Error("Error setting value.", err)
		return
	}

	removeContext, cancelRemove := context.WithCancel(context.Background())
	defer cancelRemove()
	if err := testClient.RemoveSource(removeContext, testColorsSource); err != nil {
		t.Error("Error removing source.", err)
		return
	}

	getSourcesContext, cancelGetSources := context.WithCancel(context.Background())
	defer cancelGetSources()
	sources, err := testClient.GetSources(getSourcesContext)
	if err != nil {
		t.Error("Error getting sources.", err)
		return
	}

	if len(sources) > 0 {
		t.Error("Test should have removed all sources.")
		return
	}
}

func TestRemoveValue(t *testing.T) {
	deleteTestSources()

	test := iris.KeyValuePair{Key: "primary", Value: []byte("red")}
	setContext, cancelSet := context.WithCancel(context.Background())
	defer cancelSet()
	if err := testClient.SetValue(setContext, testColorsSource, test.Key, test.Value); err != nil {
		t.Error("Error setting value.", err)
		return
	}

	removeContext, cancelRemove := context.WithCancel(context.Background())
	defer cancelRemove()
	if err := testClient.RemoveValue(removeContext, testColorsSource, test.Key); err != nil {
		t.Error("Error removing value for key.", err)
		return
	}

	getKeysContext, cancelGetKeys := context.WithCancel(context.Background())
	defer cancelGetKeys()
	keys, err := testClient.GetKeys(getKeysContext, testColorsSource)
	if err != nil {
		t.Error("Error getting keys for the test source.", err)
		return
	}

	if len(keys) > 0 {
		t.Error("Test should have removed all keys for the test source.")
		return
	}
}
