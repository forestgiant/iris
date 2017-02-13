package transport

import (
	"context"
	"errors"
	"net"
	"strconv"

	iris_api "gitlab.fg/otis/iris/api"
	"gitlab.fg/otis/iris/pb"
)

var errProxyLeader = errors.New("Unable to determine appropriate proxy address for raft cluster leader")

func getProxyAddress(leaderRaftAddr string) string {
	host, portString, err := net.SplitHostPort(leaderRaftAddr)
	p, err := strconv.Atoi(portString)
	if err != nil {
		return ""
	}
	return net.JoinHostPort(host, strconv.Itoa(p+1))
}

func proxyJoin(ctx context.Context, req *pb.JoinRequest, addr string) (*pb.JoinResponse, error) {
	proxyAddr := getProxyAddress(addr)
	client, err := iris_api.NewTLSClient(ctx, proxyAddr, "iris.forestgiant.com", "ca.cer")
	if err != nil {
		return nil, err
	}
	defer client.Close()

	if err := client.Join(ctx, req.Address); err != nil {
		return nil, err
	}

	return &pb.JoinResponse{}, nil
}

func proxySetValue(ctx context.Context, req *pb.SetValueRequest, addr string) (*pb.SetValueResponse, error) {
	proxyAddr := getProxyAddress(addr)
	client, err := iris_api.NewTLSClient(ctx, proxyAddr, "iris.forestgiant.com", "ca.cer")
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

func proxyGetValue(ctx context.Context, req *pb.GetValueRequest, addr string) (*pb.GetValueResponse, error) {
	proxyAddr := getProxyAddress(addr)
	client, err := iris_api.NewTLSClient(ctx, proxyAddr, "iris.forestgiant.com", "ca.cer")
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

func proxyRemoveValue(ctx context.Context, req *pb.RemoveValueRequest, addr string) (*pb.RemoveValueResponse, error) {
	proxyAddr := getProxyAddress(addr)
	client, err := iris_api.NewTLSClient(ctx, proxyAddr, "iris.forestgiant.com", "ca.cer")
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

func proxyRemoveSource(ctx context.Context, req *pb.RemoveSourceRequest, addr string) (*pb.RemoveSourceResponse, error) {
	proxyAddr := getProxyAddress(addr)
	client, err := iris_api.NewTLSClient(ctx, proxyAddr, "iris.forestgiant.com", "ca.cer")
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
