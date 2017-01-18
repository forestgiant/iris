package client

import (
	"io"

	"gitlab.fg/otis/sourcehub/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

//Client implements the generated sourcehub.SourceHubClient interface
type Client struct {
	conn *grpc.ClientConn
	rpc  pb.SourceHubClient
}

//NewClient returns a new sourcehub grpc client
func NewClient(ctx context.Context, serverAddress string) (*Client, error) {
	var err error
	c := &Client{}

	opts := []grpc.DialOption{grpc.WithInsecure()}
	if c.conn, err = grpc.Dial(serverAddress, opts...); err != nil {
		return nil, err
	}

	c.rpc = pb.NewSourceHubClient(c.conn)
	return c, nil
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
