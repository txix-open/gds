package ws

import (
	"github.com/integration-system/gds/cluster"
	etp "github.com/integration-system/isp-etp-go"
	log "github.com/integration-system/isp-log"
	jsoniter "github.com/json-iterator/go"
)

var (
	json = jsoniter.ConfigFastest
)

type SocketEventHandler struct {
	server        etp.Server
	clusterClient *cluster.Client
}

func (h *SocketEventHandler) SubscribeAll() {
	h.server.
		OnConnect(h.handleConnect).
		OnDisconnect(h.handleDisconnect).
		OnError(h.handleError).
		OnWithAck(cluster.ApplyCommandEvent, h.applyCommandOnLeader)
}

func (h *SocketEventHandler) handleConnect(conn etp.Conn) {
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
	log.Debugf(0, "isp-etp: %v", err)
}

func NewSocketEventHandler(server etp.Server, client *cluster.Client) *SocketEventHandler {
	return &SocketEventHandler{
		server:        server,
		clusterClient: client,
	}
}
