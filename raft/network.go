package raft

import (
	"github.com/hashicorp/raft"
	"net"
	"time"
)

type StreamLayer struct {
	net.Listener
}

func (r *StreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

type transparentAddressProvider struct {
}

func (t transparentAddressProvider) ServerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	return raft.ServerAddress(id), nil
}
