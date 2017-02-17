## Iris Client
This package can be used to communicate with an Iris server using the gRPC protocol and the Go programming language.  For usage examples, see the included `example_test.go`.

### NewClient
NewClient returns a new Iris GRPC client for the given server address. The client's Close method should be called when the returned client is no longer needed.
```
func NewClient(ctx context.Context, serverAddress string, opts []grpc.DialOption) (*Client, error)
```

###  NewTLSClient
NewTLSClient returns a new Iris GRPC client for the given server address. The certificateAuthority field allows you to provide a root certificate authority to use when verifying the remote server's identity. The serverNameOverride field is for testing only. If set to a non empty string, it will override the virtual host name of authority (e.g. :authority header field) in requests. This field is ignored if a certificateAuthority is not provided, which is interpreted as the desire to establish an insecure connection. The client's Close method should be called when the returned client is no longer needed.
```
NewTLSClient(ctx context.Context, serverAddress string, serverNameOverride string, certificateAuthority string) (*Client, error)
```

### Close
Close tears down the client's underlying connections
```
func (c *Client) Close() error
```

### Join
Join the node reachable at the address to this cluster
```
func (c *Client) Join(ctx context.Context, address string) error
```

### GetSources
GetSources responds with an array of strings representing sources
```
func (c *Client) GetSources(ctx context.Context) ([]string, error)
```

### GetKeys
GetKeys expects a source and responds with an array of strings representing the available keys
```
func (c *Client) GetKeys(ctx context.Context, source string) ([]string, error)
```

### SetValue
SetValue sets the value for the specified source and key
```
func (c *Client) SetValue(ctx context.Context, source string, key string, value []byte) error
```

### GetValue
GetValue expects a source and key and responds with the associated value
```
func (c *Client) GetValue(ctx context.Context, source string, key string) ([]byte, error)
```

### RemoveValue
RemoveValue expects a source and key and removes that entry from the source
```
func (c *Client) RemoveValue(ctx context.Context, source string, key string) error
```

### RemoveSource
RemoveSource removes the specified source and all its values from the server
```
func (c *Client) RemoveSource(ctx context.Context, source string) error
```

### Subscribe
Subscribe indicates that the client wishes to be notified of all updates for the specified source
```
func (c *Client) Subscribe(ctx context.Context, source string, handler *UpdateHandler) (*pb.SubscribeResponse, error)
```

### SubscribeKey
SubscribeKey indicates that the client wishes to be notified of updates associated with a specific key from the specified source
```
func (c *Client) SubscribeKey(ctx context.Context, source string, key string, handler *UpdateHandler) (*pb.SubscribeKeyResponse, error)
```

### Unsubscribe
Unsubscribe indicates that the client no longer wishes to be notified of updates for the specified source
```
func (c *Client) Unsubscribe(ctx context.Context, source string, handler *UpdateHandler) (*pb.UnsubscribeResponse, error)
```

### UnsubscribeKey
UnsubscribeKey indicates that the client no longer wishes to be notified of updates associated with a specific key from the specified source
```
func (c *Client) UnsubscribeKey(ctx context.Context, source string, key string, handler *UpdateHandler) (*pb.UnsubscribeKeyResponse, error)
```