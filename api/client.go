package api

import (
	"fmt"
	"io"

	"gitlab.fg/otis/sourcehub/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Client implements the generated sourcehub.SourceHubClient interface
type Client struct {
	conn *grpc.ClientConn
	rpc  pb.SourceHubClient
}

// NewClient returns a new sourcehub GRPC client for the given server address.
// The client's Close method should be called when the returned client is no longer needed.
func NewClient(ctx context.Context, serverAddress string, opts []grpc.DialOption) (*Client, error) {
	var err error
	c := &Client{}

	if len(opts) == 0 {
		opts = append(opts, grpc.WithInsecure())
	}

	if c.conn, err = grpc.Dial(serverAddress, opts...); err != nil {
		return nil, err
	}

	c.rpc = pb.NewSourceHubClient(c.conn)
	return c, nil
}

// NewTLSClient returns a new sourcehub GRPC client for the given server address.
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

// Close tears down the client's underlying connections
func (c *Client) Close() error {
	return c.conn.Close()
}

// GetSources responds with an array of strings representing sources
func (c *Client) GetSources(ctx context.Context) ([]string, error) {
	stream, err := c.rpc.GetSources(ctx, &pb.GetSourcesRequest{})
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

// GetValue expects a source and key and responds with the associated value
func (c *Client) GetValue(ctx context.Context, source string, key string) ([]byte, error) {
	resp, err := c.rpc.GetValue(ctx, &pb.GetValueRequest{
		Source: source,
		Key:    key,
	})
	return resp.Value, err
}

// SetValue sets the value for the specified source and key
func (c *Client) SetValue(ctx context.Context, source string, key string, value []byte) error {
	_, err := c.rpc.SetValue(ctx, &pb.SetValueRequest{
		Source: source,
		Key:    key,
		Value:  value,
	})
	return err
}

// GetKeys expects a source and responds with an array of strings representing the available keys
func (c *Client) GetKeys(ctx context.Context, source string) ([]string, error) {
	stream, err := c.rpc.GetKeys(ctx, &pb.GetKeysRequest{Source: source})
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

// Subscribe streams updates to a value for a given source and key
func (c *Client) Subscribe(ctx context.Context, source string, key string, handler func(value []byte)) error {
	stream, err := c.rpc.Subscribe(ctx, &pb.SubscribeRequest{
		Source: source,
		Key:    key,
	})

	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}
		handler(resp.Value)
	}

	return err
}
