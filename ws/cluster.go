package ws

import (
	"fmt"
	"github.com/integration-system/gds/cluster"
	etp "github.com/integration-system/isp-etp-go"
)

func (h *SocketEventHandler) applyCommandOnLeader(_ etp.Conn, cmd []byte) []byte {
	defer func() {
		if err := recover(); err != nil {
			h.logger.Error(fmt.Sprintf("panic: %v", err))
		}
	}()
	cmdCopy := make([]byte, len(cmd))
	copy(cmdCopy, cmd)
	obj, err := h.clusterClient.SyncApplyOnLeader(cmdCopy)
	if err != nil {
		var logResponse cluster.ApplyLogResponse
		logResponse.ApplyError = err.Error()
		data, err := json.Marshal(obj)
		if err != nil {
			panic(fmt.Errorf("marshaling ApplyLogResponse: %v", err))
		}
		return data
	}
	data, err := json.Marshal(obj)
	if err != nil {
		panic(fmt.Errorf("marshaling ApplyLogResponse: %v", err))
	}
	return data
}

func GetPeerID(conn etp.Conn) string {
	peerID := conn.URL().Query().Get(cluster.PeerIDGetParam)
	return peerID
}
