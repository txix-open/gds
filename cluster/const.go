package cluster

import (
	"bytes"
	"encoding/binary"
	log "github.com/integration-system/isp-log"
	"time"
)

const (
	ApplyCommandEvent = "CONFIG_CLUSTER:APPLY_COMMAND"

	WebsocketURLPath = "/gds/"

	PeerIDGetParam = "peer_id"
)
const (
	_ = iota
	AddPeerCommand
	RemovePeerCommand

	InsertJobCommand
	DeleteJobCommand
	AcquireJobCommand
	JobExecutedCommand
)

func PrepareAddPeerCommand(peerID string) []byte {
	return prepareCommand(AddPeerCommand, AddPeer{
		PeerID: peerID,
	})
}

func PrepareRemovePeerCommand(peerID string) []byte {
	return prepareCommand(RemovePeerCommand, RemovePeer{
		PeerID: peerID,
	})
}

func PrepareInsertJobCommand(jobType string, data []byte) []byte {
	return prepareCommand(InsertJobCommand, InsertJob{
		Job:  data,
		Type: jobType,
	})
}

func PrepareDeleteJobCommand(key string) []byte {
	return prepareCommand(DeleteJobCommand, DeleteJob{Key: key})
}

func PrepareAcquireJobCommand(jobKeys []string, peerID string) []byte {
	return prepareCommand(AcquireJobCommand, AcquireJob{
		JobKeys: jobKeys,
		PeerID:  peerID,
	})
}

func PrepareJobExecutedCommand(jobKey, err string, executedTime time.Time) []byte {
	return prepareCommand(JobExecutedCommand, JobExecuted{
		JobKey:       jobKey,
		Error:        err,
		ExecutedTime: executedTime,
	})
}

func prepareCommand(command uint64, payload interface{}) []byte {
	cmd := make([]byte, 8, 256)
	binary.BigEndian.PutUint64(cmd, command)
	buf := bytes.NewBuffer(cmd)
	err := json.NewEncoder(buf).Encode(payload)
	if err != nil {
		log.Fatalf(0, "prepare log command: %v", err)
	}
	return buf.Bytes()
}
