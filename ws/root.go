package ws

import (
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/integration-system/gds/cluster"
	etp "github.com/integration-system/isp-etp-go"
	jsoniter "github.com/json-iterator/go"
)

var (
	json = jsoniter.ConfigFastest
)

type SocketEventHandler struct {
	server        etp.Server
	clusterClient *cluster.Client
	logger        hclog.Logger
}

func (h *SocketEventHandler) SubscribeAll() {
	h.server.
		OnConnect(h.handleConnect).
		OnDisconnect(h.handleDisconnect).
		OnError(h.handleError).
		OnWithAck(cluster.ApplyCommandEvent, h.applyCommandOnLeader)
}

func (h *SocketEventHandler) handleConnect(conn etp.Conn) {
	defer func() {
		if err := recover(); err != nil {
			h.logger.Error(fmt.Sprintf("panic on ws connect: %v", err))
		}
	}()
	peerID := GetPeerID(conn)
	if peerID == "" {
		return
	}

	command := cluster.PrepareAddPeerCommand(peerID)
	_, err := h.clusterClient.SyncApplyHelper(command, "AddPeerCommand")
	if err != nil {
		_ = conn.Close()
	}
}

func (h *SocketEventHandler) handleDisconnect(conn etp.Conn, _ error) {
	defer func() {
		if err := recover(); err != nil {
			h.logger.Error(fmt.Sprintf("panic on ws disconnect: %v", err))
		}
	}()
	peerID := GetPeerID(conn)
	if peerID == "" {
		return
	}

	command := cluster.PrepareRemovePeerCommand(peerID)
	_, err := h.clusterClient.SyncApplyHelper(command, "RemovePeerCommand")
	if err != nil {
		_ = conn.Close()
	}
}

func (h *SocketEventHandler) handleError(_ etp.Conn, err error) {
	h.logger.Debug(fmt.Sprintf("isp-etp: %v", err))
}

func NewSocketEventHandler(server etp.Server, client *cluster.Client, logger hclog.Logger) *SocketEventHandler {
	return &SocketEventHandler{
		server:        server,
		clusterClient: client,
		logger:        logger,
	}
}
