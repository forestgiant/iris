package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	fggrpclog "github.com/forestgiant/grpclog"
	"gitlab.fg/otis/iris/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
)

func init() {
	grpclog.SetLogger(&fggrpclog.Suppressed{})
}

// UpdateHandler descibes a function used for handling values received by the client
type UpdateHandler func(update *pb.Update)

// Client for communicating with an Iris server
type Client struct {
	initialized  bool
	conn         *grpc.ClientConn
	rpc          pb.IrisClient
	listenStream pb.Iris_ListenClient

	session             string
	sourceHandlersMutex *sync.Mutex
	sourceHandlers      map[string][]*UpdateHandler
	keyHandlersMutex    *sync.Mutex
	keyHandlers         map[string]map[string][]*UpdateHandler
}

// NewClient returns a new Iris GRPC client for the given server address.
// The client's Close method should be called when the returned client is no longer needed.
func NewClient(ctx context.Context, serverAddress string, opts []grpc.DialOption) (*Client, error) {
	if len(serverAddress) == 0 {
		return nil, errors.New("You must provide a server address to connect to")
	}

	var err error
	c := &Client{}

	if len(opts) == 0 {
		opts = append(opts, grpc.WithInsecure())
	}

	opts = append(opts, grpc.FailOnNonTempDialError(true))
	opts = append(opts, grpc.WithBlock())

	if c.conn, err = grpc.Dial(serverAddress, opts...); err != nil {
		return nil, err
	}

	c.rpc = pb.NewIrisClient(c.conn)
	resp, err := c.rpc.Connect(ctx, &pb.ConnectRequest{})
	if err != nil {
		return c, err
	}
	c.session = resp.Session

	if err := c.listen(context.Background()); err != nil {
		return nil, err
	}

	return c, nil
}

// NewTLSClient returns a new Iris GRPC client for the given server address.
// The certificateAuthority field allows you to provide a root certificate authority
// to use when verifying the remote server's identity.
// The serverNameOverride field is for testing only. If set to a non empty string,
// it will override the virtual host name of authority (e.g. :authority header field)
// in requests. This field is ignored if a certificateAuthority is not provided,
// which is interpreted as the desire to establish an insecure connection.
// The client's Close method should be called when the returned client is no longer needed.
func NewTLSClient(ctx context.Context, serverAddress string, serverNameOverride string, certificateAuthority string) (*Client, error) {
	var opts []grpc.DialOption
	if len(certificateAuthority) > 0 {
		creds, err := credentials.NewClientTLSFromFile(certificateAuthority, serverNameOverride)
		if err != nil {
			return nil, fmt.Errorf("Failed to generate credentials %v", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}

	return NewClient(ctx, serverAddress, opts)
}

func (c *Client) initialize() {
	if c.initialized {
		return
	}

	c.initialized = true
	c.sourceHandlersMutex = &sync.Mutex{}
	c.keyHandlersMutex = &sync.Mutex{}
}

//Join the node reachable at the address to this cluster
func (c *Client) Join(ctx context.Context, address string) error {
	if _, err := c.rpc.Join(ctx, &pb.JoinRequest{Address: address}); err != nil {
		return err
	}
	return nil
}

// Close tears down the client's underlying connections
func (c *Client) Close() error {
	c.initialize()

	c.session = ""

	c.sourceHandlersMutex.Lock()
	defer c.sourceHandlersMutex.Unlock()

	c.sourceHandlers = nil

	c.keyHandlersMutex.Lock()
	defer c.keyHandlersMutex.Unlock()

	c.keyHandlers = nil

	return c.conn.Close()
}

// Listen responds with a stream of objects representing source, key, value updates
func (c *Client) listen(ctx context.Context) error {
	c.initialize()

	req := &pb.ListenRequest{
		Session: c.session,
	}

	var err error
	c.listenStream, err = c.rpc.Listen(ctx, req)
	if err != nil {
		return err
	}

	go func() {
		for {
			resp, err := c.listenStream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}

				return
			}

			shs := c.sourceHandlers[resp.Source]
			khs := c.keyHandlers[resp.Source][resp.Key]

			go func(update *pb.Update, sourceHandlers []*UpdateHandler, keyHandlers []*UpdateHandler) {
				for _, h := range sourceHandlers {
					go (*h)(resp)
				}

				for _, h := range keyHandlers {
					go (*h)(resp)
				}
			}(resp, shs, khs)
		}
	}()

	return nil
}

// GetSources responds with an array of strings representing sources
func (c *Client) GetSources(ctx context.Context) ([]string, error) {
	c.initialize()

	stream, err := c.rpc.GetSources(ctx, &pb.GetSourcesRequest{
		Session: c.session,
	})

	if err != nil {
		return nil, err
	}

	var sources []string
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}
		sources = append(sources, resp.Source)
	}

	return sources, nil
}

// GetKeys expects a source and responds with an array of strings representing the available keys
func (c *Client) GetKeys(ctx context.Context, source string) ([]string, error) {
	c.initialize()

	stream, err := c.rpc.GetKeys(ctx, &pb.GetKeysRequest{
		Session: c.session,
		Source:  source,
	})

	if err != nil {
		return nil, err
	}

	var keys []string
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}
		keys = append(keys, resp.Key)
	}

	return keys, nil
}

// SetValue sets the value for the specified source and key
func (c *Client) SetValue(ctx context.Context, source string, key string, value []byte) error {
	c.initialize()

	_, err := c.rpc.SetValue(ctx, &pb.SetValueRequest{
		Session: c.session,
		Source:  source,
		Key:     key,
		Value:   value,
	})
	return err
}

// GetValue expects a source and key and responds with the associated value
func (c *Client) GetValue(ctx context.Context, source string, key string) ([]byte, error) {
	c.initialize()

	resp, err := c.rpc.GetValue(ctx, &pb.GetValueRequest{
		Session: c.session,
		Source:  source,
		Key:     key,
	})

	if resp == nil {
		return nil, err
	}

	return resp.Value, err
}

// RemoveValue expects a source and key and removes that entry from the source
func (c *Client) RemoveValue(ctx context.Context, source string, key string) error {
	c.initialize()

	_, err := c.rpc.RemoveValue(ctx, &pb.RemoveValueRequest{
		Session: c.session,
		Source:  source,
		Key:     key,
	})
	return err
}

// RemoveSource removes the specified source and all its values from the server
func (c *Client) RemoveSource(ctx context.Context, source string) error {
	c.initialize()

	_, err := c.rpc.RemoveSource(ctx, &pb.RemoveSourceRequest{
		Session: c.session,
		Source:  source,
	})
	return err
}

// Subscribe indicates that the client wishes to be notified of all updates for the specified source
func (c *Client) Subscribe(ctx context.Context, source string, handler *UpdateHandler) (*pb.SubscribeResponse, error) {
	c.initialize()

	c.sourceHandlersMutex.Lock()
	defer c.sourceHandlersMutex.Unlock()

	if c.sourceHandlers == nil {
		c.sourceHandlers = make(map[string][]*UpdateHandler)
	}

	if c.sourceHandlers[source] == nil {
		c.sourceHandlers[source] = []*UpdateHandler{}
	}
	c.sourceHandlers[source] = append(c.sourceHandlers[source], handler)

	return c.rpc.Subscribe(ctx, &pb.SubscribeRequest{
		Session: c.session,
		Source:  source,
	})
}

// SubscribeKey indicates that the client wishes to be notified of updates associated with
// a specific key from the specified source
func (c *Client) SubscribeKey(ctx context.Context, source string, key string, handler *UpdateHandler) (*pb.SubscribeKeyResponse, error) {
	c.initialize()

	c.keyHandlersMutex.Lock()
	defer c.keyHandlersMutex.Unlock()

	if c.keyHandlers == nil {
		c.keyHandlers = make(map[string]map[string][]*UpdateHandler)
	}

	if c.keyHandlers[source] == nil {
		c.keyHandlers[source] = make(map[string][]*UpdateHandler)
	}

	c.keyHandlers[source][key] = append(c.keyHandlers[source][key], handler)

	return c.rpc.SubscribeKey(ctx, &pb.SubscribeKeyRequest{
		Session: c.session,
		Source:  source,
		Key:     key,
	})
}

// Unsubscribe indicates that the client no longer wishes to be notified of updates for the specified source
func (c *Client) Unsubscribe(ctx context.Context, source string, handler *UpdateHandler) (*pb.UnsubscribeResponse, error) {
	c.initialize()

	c.sourceHandlersMutex.Lock()
	defer c.sourceHandlersMutex.Unlock()

	if c.sourceHandlers != nil && c.sourceHandlers[source] != nil {
		c.sourceHandlers[source] = removeHandler(handler, c.sourceHandlers[source])

		if len(c.sourceHandlers[source]) > 0 {
			return &pb.UnsubscribeResponse{
				Source: source,
			}, nil
		}
	}

	return c.rpc.Unsubscribe(ctx, &pb.UnsubscribeRequest{
		Session: c.session,
		Source:  source,
	})
}

// UnsubscribeKey indicates that the client no longer wishes to be notified of updates associated
// with a specific key from the specified source
func (c *Client) UnsubscribeKey(ctx context.Context, source string, key string, handler *UpdateHandler) (*pb.UnsubscribeKeyResponse, error) {
	c.initialize()

	c.keyHandlersMutex.Lock()
	defer c.keyHandlersMutex.Unlock()

	if c.keyHandlers != nil && c.keyHandlers[source] != nil && c.keyHandlers[source][key] != nil {
		c.keyHandlers[source][key] = removeHandler(handler, c.keyHandlers[source][key])

		if len(c.keyHandlers[source][key]) > 0 {
			return &pb.UnsubscribeKeyResponse{
				Source: source,
				Key:    key,
			}, nil
		}
	}

	return c.rpc.UnsubscribeKey(ctx, &pb.UnsubscribeKeyRequest{
		Session: c.session,
		Source:  source,
		Key:     key,
	})
}

// RemoveHandler removes the specified handler from the collection
func removeHandler(handler *UpdateHandler, handlers []*UpdateHandler) []*UpdateHandler {
	index := -1
	for i, h := range handlers {
		if h == handler {
			index = i
		}
	}

	if index >= 0 {
		handlers = append(handlers[:index], handlers[index+1:]...)
	}

	return handlers
}
