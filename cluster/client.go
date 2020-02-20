package cluster

import (
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/integration-system/gds/raft"
	"github.com/integration-system/gds/utils"
	jsoniter "github.com/json-iterator/go"
	"sync"
	"time"
)

var (
	ErrNoLeader                   = errors.New("no leader in cluster")
	ErrLeaderClientNotInitialized = errors.New("leader client not initialized")
	ErrNotLeader                  = errors.New("node is not a leader")
	json                          = jsoniter.ConfigFastest
)

const (
	leaderConnectionTimeout = 3 * time.Second
	defaultApplyTimeout     = 3 * time.Second
)

type Client struct {
	r *raft.Raft

	leaderMu      sync.RWMutex
	leaderState   leaderState
	leaderClient  *SocketLeaderClient
	prevLeaderIds []string
	leaderCh      chan bool

	logger hclog.Logger
}

func (client *Client) Shutdown() error {
	client.leaderMu.Lock()
	defer client.leaderMu.Unlock()

	if client.leaderClient != nil {
		client.leaderClient.Close()
		client.leaderClient = nil
	}

	return client.r.GracefulShutdown()
}

func (client *Client) IsLeader() bool {
	client.leaderMu.RLock()
	leader := client.leaderState.isLeader
	client.leaderMu.RUnlock()

	return leader
}

func (client *Client) LocalID() string {
	return client.r.LocalID()
}

func (client *Client) Leader() string {
	return string(client.r.Leader())
}

func (client *Client) Servers() ([]string, error) {
	// TODO: return active servers instead
	servers, err := client.r.Servers()
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(servers))
	for _, server := range servers {
		ids = append(ids, string(server.ID))
	}
	return ids, nil
}

func (client *Client) LeaderCh() <-chan bool {
	return client.leaderCh
}

func (client *Client) SyncApply(command []byte) (*ApplyLogResponse, error) {
	client.leaderMu.RLock()
	defer client.leaderMu.RUnlock()

	if !client.leaderState.leaderElected {
		return nil, ErrNoLeader
	}

	if client.leaderState.isLeader {
		apply, err := client.r.SyncApply(command)
		if err != nil {
			return nil, err
		}
		logResponse := apply.(ApplyLogResponse)
		return &logResponse, err
	}

	if client.leaderClient == nil {
		return nil, ErrLeaderClientNotInitialized
	}
	response, err := client.leaderClient.Ack(command, defaultApplyTimeout)
	if err != nil {
		return nil, err
	}
	var logResponse ApplyLogResponse
	err = json.Unmarshal(response, &logResponse)
	if err != nil {
		return nil, err
	}
	return &logResponse, nil
}

func (client *Client) SyncApplyOnLeader(command []byte) (*ApplyLogResponse, error) {
	client.leaderMu.RLock()
	defer client.leaderMu.RUnlock()

	if !client.leaderState.isLeader {
		return nil, ErrNotLeader
	}
	apply, err := client.r.SyncApply(command)
	if err != nil {
		return nil, err
	}
	logResponse := apply.(ApplyLogResponse)
	return &logResponse, err
}

func (client *Client) SyncApplyHelper(command []byte, commandName string) (interface{}, error) {
	applyLogResponse, err := client.SyncApply(command)
	if err != nil {
		client.logger.Warn(fmt.Sprintf("apply %s: %v", commandName, err))
		return nil, err
	}
	if applyLogResponse != nil && applyLogResponse.ApplyError != "" {
		client.logger.Warn("apply command",
			"result", string(applyLogResponse.Result),
			"applyError", applyLogResponse.ApplyError,
			"commandName", commandName,
		)
		return applyLogResponse.Result, errors.New(applyLogResponse.ApplyError)
	}
	return applyLogResponse.Result, nil
}

func (client *Client) listenLeaderNotifications() {
	defer func() {
		if err := recover(); err != nil {
			client.logger.Error(fmt.Sprintf("panic: %v", err))
		}
		close(client.leaderCh)
	}()

	for n := range client.r.LeaderNotificationsCh() {
		client.leaderMu.Lock()
		if client.leaderClient != nil {
			client.logger.Debug(fmt.Sprintf("close previous leader ws connection %s", client.leaderState.leaderAddr))
			client.leaderClient.Close()
			client.leaderClient = nil
		}

		if !client.leaderState.isLeader && n.IsLeader {
			client.leaderCh <- true
		} else if client.leaderState.isLeader && !n.IsLeader {
			client.leaderCh <- false
		}

		if !n.LeaderElected {
			if client.leaderState.leaderAddr != "" {
				client.prevLeaderIds = append(client.prevLeaderIds, client.leaderState.leaderAddr)
			}
		} else {
			if n.IsLeader {
				go func(ids []string) {
					if len(ids) > 0 {
						uniqueIds := utils.MakeUnique(ids)
						for _, id := range uniqueIds {
							cmd := PrepareRemovePeerCommand(id)
							_, _ = client.SyncApplyHelper(cmd, "RemovePeerCommand")
						}
					}

					cmd := PrepareAddPeerCommand(client.LocalID())
					_, _ = client.SyncApplyHelper(cmd, "AddPeerCommand")
				}(client.prevLeaderIds)
			} else {
				leaderClient := NewSocketLeaderClient(n.CurrentLeaderAddress, client.r.LocalID(), client.logger)
				if err := leaderClient.Dial(leaderConnectionTimeout); err != nil {
					client.logger.Error(fmt.Sprintf("could not connect to leader: %v", err))
					continue
				}
				client.leaderClient = leaderClient
			}
			// For now, if new leader had no time to push events about previous leaders, these events will be lost.
			client.prevLeaderIds = nil
		}

		client.leaderState = leaderState{
			leaderElected: n.LeaderElected,
			isLeader:      n.IsLeader,
			leaderAddr:    n.CurrentLeaderAddress,
		}

		client.leaderMu.Unlock()
	}
}

type leaderState struct {
	leaderElected bool
	isLeader      bool
	leaderAddr    string
}

func NewRaftClusterClient(r *raft.Raft, logger hclog.Logger) *Client {
	client := &Client{
		r:           r,
		leaderState: leaderState{},
		leaderCh:    make(chan bool, 5),
		logger:      logger,
	}
	go client.listenLeaderNotifications()

	return client
}
