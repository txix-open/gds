package cluster

import (
	json2 "encoding/json"
	"time"
)

type ApplyLogResponse struct {
	ApplyError string
	Result     json2.RawMessage
}

func (a ApplyLogResponse) Error() string {
	return a.ApplyError
}

type InsertJob struct {
	Job  json2.RawMessage
	Type string
}

type DeleteJob struct {
	Key string
}

type AcquireJob struct {
	JobKeys []string
	PeerID  string
}

type JobExecuted struct {
	JobKey       string
	Error        string
	ExecutedTime time.Time
}

type AddPeer struct {
	PeerID string
}

type RemovePeer struct {
	PeerID string
}
