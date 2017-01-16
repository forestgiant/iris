package client

import (
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
func (c *Client) GetValue(ctx context.Context, req *pb.GetValueRequest, opts ...grpc.CallOption) (*pb.GetValueResponse, error) {
	return c.rpc.GetValue(ctx, req, opts...)
}

// SetValue sets the value for the specified source and key
func (c *Client) SetValue(ctx context.Context, req *pb.SetValueRequest, opts ...grpc.CallOption) (*pb.SetValueResponse, error) {
	return c.rpc.SetValue(ctx, req, opts...)
}

// Subscribe streams updates to a value for a given source and key
func (c *Client) Subscribe(ctx context.Context, req *pb.SubscribeRequest, opts ...grpc.CallOption) (pb.SourceHub_SubscribeClient, error) {
	return c.rpc.Subscribe(ctx, req, opts...)
}
