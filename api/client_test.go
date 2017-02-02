package api

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/forestgiant/portutil"

	"google.golang.org/grpc"

	"gitlab.fg/otis/sourcehub/pb"
	"gitlab.fg/otis/sourcehub/transport"
)

var testClient *Client
var testServiceAddress string

const testColorsSource = "com.forestgiant.sourcehub.testing.colors"
const testSoundsSource = "com.forestgiant.sourcehub.testing.sounds"

func TestMain(m *testing.M) {
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

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterSourceHubServer(grpcServer, &transport.Server{})
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
			}
		}

		if test.ShouldError && err == nil {
			t.Error("NewClient should produce an error when provided an invalid server address")
		} else if !test.ShouldError && err != nil {
			t.Error("NewClient should not have produced error.", err)
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
			}

			if len(test.Value) != len(value) {
				t.Errorf("Error getting value. Received values does not match sent value in length")
			}

			for i := range test.Value {
				if test.Value[i] != value[i] {
					t.Errorf("Error getting value. Received values does not match sent value")
					break
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
		}

		for _, s := range sources {
			if _, ok := expected[s]; !ok {
				t.Errorf("Unexpected value returned in sources.  Received %s.", s)
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
			}

			for _, key := range keys {
				if _, ok := expected[source][key]; !ok {
					t.Error("GetKeys returned unexpected key")
				}
			}
		}

	})
}

func TestSourceSubscriptions(t *testing.T) {
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

	wg := &sync.WaitGroup{}

	var sourceSubCallback UpdateHandler = func(u *pb.Update) error {
		wg.Done()
		return nil
	}

	sourceSubCtx, cancelSourceSub := context.WithCancel(context.Background())
	defer cancelSourceSub()
	_, err := testClient.Subscribe(sourceSubCtx, testColorsSource, sourceSubCallback)
	if err != nil {
		t.Error(err)
	}

	for _, test := range tests {
		setCtx, cancelSet := context.WithCancel(context.Background())
		defer cancelSet()
		wg.Add(1)
		if err := testClient.SetValue(setCtx, test.Source, test.Key, test.Value); err != nil {
			t.Error("Failed to set test value")
			continue
		}
	}

	wg.Wait()
}
