package api

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/forestgiant/portutil"
	"google.golang.org/grpc"

	fglog "github.com/forestgiant/log"
	"gitlab.fg/otis/iris"
	"gitlab.fg/otis/iris/pb"
	"gitlab.fg/otis/iris/store"
	"gitlab.fg/otis/iris/transport"
)

var testClient *Client
var testServiceAddress string

const testColorsSource = "com.forestgiant.iris.testing.colors"
const testSoundsSource = "com.forestgiant.iris.testing.sounds"

func TestMain(m *testing.M) {
	wd, err := os.Getwd()
	if err != nil {
		fmt.Println("Unable to get current working directory", err)
		os.Exit(1)
	}

	port, err := portutil.GetUniqueTCP()
	if err != nil {
		fmt.Println("unable to obtain open port", err)
		os.Exit(1)
	}

	testServiceAddress = fmt.Sprintf("127.0.0.1:%d", port)
	l, err := net.Listen("tcp", testServiceAddress)
	if err != nil {
		fmt.Println("unable to start tcp listener", err)
		os.Exit(1)
	}

	testRaftDir := filepath.Join(wd, "com.forestgiant.iris.testing.client.raftDir")
	var store = store.NewStore(":12000", testRaftDir, fglog.Logger{})
	if err := store.Open(true); err != nil {
		fmt.Println("Failed to open data store.", err)
		//TODO: Find a way to defer this call (another call below as well)
		os.RemoveAll(testRaftDir)
		os.Exit(1)
	}

	//TODO: Find a better way to ensure there is a leader
	time.Sleep(3 * time.Second)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterIrisServer(grpcServer, &transport.Server{
		Store: store,
	})
	errchan := make(chan error)
	go func() {
		errchan <- grpcServer.Serve(l)
	}()

	statusChan := make(chan int)
	go func() {
		clientCtx, cancelClient := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancelClient()
		testClient, err = NewClient(clientCtx, testServiceAddress, nil)
		if err != nil {
			log.Fatal(err)
		}

		statusChan <- m.Run()

	}()

	for {
		select {
		case err := <-errchan:
			fmt.Println(err)
		case status := <-statusChan:
			if err := testClient.Close(); err != nil {
				log.Fatal(err)
			}
			//TODO: Find a way to defer this call (another call above as well)
			os.RemoveAll(testRaftDir)
			os.Exit(status)
		}
	}
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

		c, err := NewClient(ctx, test.Address, nil)
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

	var tests = []struct {
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

		var tests = []struct {
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

		wg := &sync.WaitGroup{}

		var sourceSubCallback UpdateHandler = func(u *pb.Update) error {
			if u.Source != testColorsSource {
				t.Error("Received update for ", u.Source, "source, but should only receive updates for the", testColorsSource, "source")
			} else {
				wg.Done()
			}
			return nil
		}

		sourceSubCtx, cancelSourceSub := context.WithCancel(context.Background())
		defer cancelSourceSub()
		_, err := testClient.Subscribe(sourceSubCtx, testColorsSource, &sourceSubCallback)
		if err != nil {
			t.Error(err)
			return
		}

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

		sourceUnsubContext, cancelUnsubSource := context.WithCancel(context.Background())
		defer cancelUnsubSource()
		_, err = testClient.Unsubscribe(sourceUnsubContext, testColorsSource, &sourceSubCallback)
		if err != nil {
			t.Error(err)
			return
		}
	})

	t.Run("TestKeySubscriptions", func(t *testing.T) {
		deleteTestSources()

		testKey := "primary"
		var tests = []struct {
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

		var keySubCallback UpdateHandler = func(u *pb.Update) error {
			if u.Source == testColorsSource && u.Key == testKey {
				wg.Done()
			} else {
				t.Error("Received update for ", u, "source, but should only receive updates for the", testColorsSource, "source and", testKey, "key")
			}
			return nil
		}

		keySubCtx, cancelKeySub := context.WithCancel(context.Background())
		defer cancelKeySub()
		_, err := testClient.SubscribeKey(keySubCtx, testColorsSource, testKey, &keySubCallback)
		if err != nil {
			t.Error(err)
			return
		}

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

		keyUnsubContext, cancelUnsubSource := context.WithCancel(context.Background())
		defer cancelUnsubSource()
		_, err = testClient.UnsubscribeKey(keyUnsubContext, testColorsSource, testKey, &keySubCallback)
		if err != nil {
			t.Error(err)
			return
		}
	})
}

func TestRemoveHandler(t *testing.T) {
	var handler1 UpdateHandler = func(u *pb.Update) error { fmt.Println("handler1"); return nil }
	var handler2 UpdateHandler = func(u *pb.Update) error { fmt.Println("handler2"); return nil }
	var handler3 UpdateHandler = func(u *pb.Update) error { fmt.Println("handler3"); return nil }
	var handlers = []*UpdateHandler{&handler1, &handler2, &handler3}

	count := len(handlers)
	for i := 0; i < count; i++ {
		handlers = removeHandler(handlers[len(handlers)-1], handlers)
	}

	if len(handlers) > 0 {
		t.Error("Handlers array should have no handlers left after removal.  Found", count)
		return
	}
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
