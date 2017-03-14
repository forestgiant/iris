package transport

import (
	"context"
	"errors"
	"net"
	"strconv"

	iris_api "github.com/forestgiant/iris/api"
	"github.com/forestgiant/iris/pb"
)

//Proxy is used to redirect request to an alternate Iris instance
type Proxy struct {
	ServerName string
	CertPath   string
	KeyPath    string
	CAPath     string
}

var errProxyLeader = errors.New("Unable to determine appropriate proxy address for raft cluster leader")

func (p *Proxy) getProxyAddress(leaderRaftAddr string) string {
	host, portString, err := net.SplitHostPort(leaderRaftAddr)
	port, err := strconv.Atoi(portString)
	if err != nil {
		return ""
	}
	return net.JoinHostPort(host, strconv.Itoa(port-1))
}

func (p *Proxy) getProxyClient(ctx context.Context, address string) (*iris_api.Client, error) {
	proxyAddr := p.getProxyAddress(address)
	return iris_api.NewTLSClient(ctx, proxyAddr, p.ServerName, p.CertPath, p.KeyPath, p.CAPath)
}

//Join is used to redirect a Join request to an alternate server
func (p *Proxy) Join(ctx context.Context, req *pb.JoinRequest, addr string) (*pb.JoinResponse, error) {
	client, err := p.getProxyClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	if err := client.Join(ctx, req.Address); err != nil {
		return nil, err
	}

	return &pb.JoinResponse{}, nil
}

//SetValue is used to redirect a SetValue request to an alternate server
func (p *Proxy) SetValue(ctx context.Context, req *pb.SetValueRequest, addr string) (*pb.SetValueResponse, error) {
	client, err := p.getProxyClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	if err := client.SetValue(ctx, req.Source, req.Key, req.Value); err != nil {
		return nil, err
	}

	return &pb.SetValueResponse{
		Value: req.Value,
	}, nil
}

//GetValue is used to redirect a GetValue request to an alternate server
func (p *Proxy) GetValue(ctx context.Context, req *pb.GetValueRequest, addr string) (*pb.GetValueResponse, error) {
	client, err := p.getProxyClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	value, err := client.GetValue(ctx, req.Source, req.Key)
	if err != nil {
		return nil, err
	}

	return &pb.GetValueResponse{
		Value: value,
	}, nil
}

//RemoveValue is used to redirect a RemoveValue request to an alternate server
func (p *Proxy) RemoveValue(ctx context.Context, req *pb.RemoveValueRequest, addr string) (*pb.RemoveValueResponse, error) {
	client, err := p.getProxyClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	if err := client.RemoveValue(ctx, req.Source, req.Key); err != nil {
		return nil, err
	}

	return &pb.RemoveValueResponse{
		Session: "",
		Source:  req.Source,
		Key:     req.Key,
	}, nil
}

//RemoveSource is used to redirect a RemoveSource request to an alternate server
func (p *Proxy) RemoveSource(ctx context.Context, req *pb.RemoveSourceRequest, addr string) (*pb.RemoveSourceResponse, error) {
	client, err := p.getProxyClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	if err := client.RemoveSource(ctx, req.Source); err != nil {
		return nil, err
	}

	return &pb.RemoveSourceResponse{
		Session: "",
		Source:  req.Source,
	}, nil
}
