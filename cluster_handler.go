package gds

import (
	"github.com/integration-system/gds/cluster"
	"github.com/integration-system/gds/jobs"
	"github.com/integration-system/gds/provider"
	"github.com/integration-system/gds/store"
	"github.com/integration-system/gds/utils"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"sync"
	"time"
)

const (
	assigningInterval = 100 * time.Millisecond
	batchSize         = 1000
)

var (
	json = jsoniter.ConfigFastest
)

type ClusterHandler struct {
	cluster      *cluster.Client
	executor     executor
	typeProvider provider.TypeProvider

	nextPeer         *utils.RoundRobinStrings
	assignJobsChLock sync.RWMutex
	assignJobsCh     chan []string
}

func (cl *ClusterHandler) HandleAddPeerCommand(state store.WritableState, data []byte) (interface{}, error) {
	payload := cluster.AddPeer{}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.AddPeer")
	}

	state.AddPeer(payload.PeerID)
	cl.nextPeer.Update(state.GetPeers())

	return nil, nil
}

func (cl *ClusterHandler) HandleRemovePeerCommand(state store.WritableState, data []byte) (interface{}, error) {
	payload := cluster.RemovePeer{}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.RemovePeer")
	}
	state.RemovePeer(payload.PeerID)
	cl.nextPeer.Update(state.GetPeers())

	if cl.cluster.IsLeader() {
		jobs := state.GetPeerJobsKeys(payload.PeerID)
		cl.assignJobs(jobs)
	}
	state.UnassignPeer(payload.PeerID)

	return nil, nil
}

func (cl *ClusterHandler) HandleInsertJobCommand(state store.WritableState, data []byte) (interface{}, error) {
	payload := cluster.InsertJob{}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.InsertJob")
	}

	f := cl.typeProvider.Get(payload.Type)
	job := f()
	err = job.Unmarshal(payload.Job)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal job data")
	}

	err = state.InsertJob(job)
	if err != nil {
		return nil, err
	}

	if cl.cluster.IsLeader() {
		cl.assignJobs([]string{job.Key()})
	}

	return nil, nil
}

func (cl *ClusterHandler) HandleDeleteJobCommand(state store.WritableState, data []byte) (interface{}, error) {
	payload := cluster.DeleteJob{}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.DeleteJob")
	}

	jobInfo, err := state.GetJob(payload.Key)
	if err != nil {
		return nil, err
	}
	if cl.cluster.LocalID() == jobInfo.AssignedPeerID {
		cl.executor.CancelJob(payload.Key)
	}
	state.DeleteJob(payload.Key)

	return nil, nil
}

func (cl *ClusterHandler) HandleAcquireJobCommand(state store.WritableState, data []byte) (interface{}, error) {
	payload := cluster.AcquireJob{}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.AcquireJob")
	}

	for _, key := range payload.JobKeys {
		jobInfo, err := state.GetJob(key)
		if err != nil {
			continue
		}

		// if duplicate assignment
		if jobInfo.AssignedPeerID == payload.PeerID {
			continue
		}

		if jobInfo.State == jobs.StateExhausted {
			continue
		}

		state.AcquireJob(key, payload.PeerID)
		if cl.cluster.LocalID() == payload.PeerID {
			cl.executor.AddJob(jobInfo.Job)
		}
	}

	return nil, nil
}

func (cl *ClusterHandler) HandleJobExecutedCommand(state store.WritableState, data []byte) (interface{}, error) {
	payload := cluster.JobExecuted{}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.JobExecuted")
	}

	state.ApplyPostExecution(payload.JobKey, payload.Error, payload.ExecutedTime)

	jobInfo, err := state.GetJob(payload.JobKey)
	if err != nil {
		return nil, err
	}
	if cl.cluster.LocalID() == jobInfo.AssignedPeerID {
		if jobInfo.State != jobs.StateExhausted {
			cl.executor.AddJob(jobInfo.Job)
		}
	}

	return nil, nil
}

func (cl *ClusterHandler) GetHandlers() map[uint64]func(store.WritableState, []byte) (interface{}, error) {
	return map[uint64]func(store.WritableState, []byte) (interface{}, error){
		cluster.AddPeerCommand:    cl.HandleAddPeerCommand,
		cluster.RemovePeerCommand: cl.HandleRemovePeerCommand,

		cluster.InsertJobCommand:   cl.HandleInsertJobCommand,
		cluster.DeleteJobCommand:   cl.HandleDeleteJobCommand,
		cluster.AcquireJobCommand:  cl.HandleAcquireJobCommand,
		cluster.JobExecutedCommand: cl.HandleJobExecutedCommand,
	}
}

func (cl *ClusterHandler) listenLeaderCh(mainStore *store.Store) {
	closeCh := make(chan struct{})
	for isLeader := range cl.cluster.LeaderCh() {
		if !isLeader {
			close(closeCh)
			continue
		}
		closeCh = make(chan struct{})
		assignJobsCh := make(chan []string, batchSize)
		// TODO get rid of mutex
		cl.assignJobsChLock.Lock()
		cl.assignJobsCh = assignJobsCh
		cl.assignJobsChLock.Unlock()

		go cl.checkPeers(closeCh, mainStore.VisitReadonlyState)
		go cl.checkJobs(closeCh, mainStore.VisitReadonlyState)
		go cl.backgroundAssigningJobs(closeCh, assignJobsCh)
	}
}

// Used to detect changes in onlinePeers while there was no leader in cluster (e.g. another peer got down).
func (cl *ClusterHandler) checkPeers(closeCh chan struct{}, visitState func(f func(store.ReadonlyState))) {
	time.Sleep(200 * time.Millisecond)

	var oldServers []string
	visitState(func(state store.ReadonlyState) {
		oldServers = state.GetPeers()
	})
	var newServers []string
	newServers, err := cl.cluster.Servers()
	// error only occurs during leadership transferring, so there will be new leader soon
	if err != nil {
		return
	}
	_, deleted := utils.CompareSlices(oldServers, newServers)

	for _, peerID := range deleted {
		select {
		case <-closeCh:
			return
		default:
		}

		cmd := cluster.PrepareRemovePeerCommand(peerID)
		_, _ = cl.cluster.SyncApplyHelper(cmd, "RemovePeerCommand")
	}
}

// Checks all jobs to have assignedPeer.
func (cl *ClusterHandler) checkJobs(closeCh chan struct{}, visitState func(f func(store.ReadonlyState))) {
	time.Sleep(300 * time.Millisecond)

	var unassignedJobsKeys []string
	visitState(func(state store.ReadonlyState) {
		unassignedJobsKeys = state.GetUnassignedJobsKeys()
	})

	select {
	case <-closeCh:
		return
	default:
	}

	cl.assignJobs(unassignedJobsKeys)
}

func (cl *ClusterHandler) backgroundAssigningJobs(closeCh chan struct{}, assignJobsCh chan []string) {
	jobKeys := make([]string, 0, batchSize)
	ticker := time.NewTicker(assigningInterval)
	defer ticker.Stop()

	sendEvents := func(keys []string) {
		peerID := cl.nextPeer.Get()
		cmd := cluster.PrepareAcquireJobCommand(keys, peerID)
		_, _ = cl.cluster.SyncApplyHelper(cmd, "AcquireJobCommand")
	}

	for {
		select {
		case newJobKeys := <-assignJobsCh:
			jobKeys = append(jobKeys, newJobKeys...)
			if len(jobKeys) >= 2*batchSize {
				// TODO send batches async in select?
				var batches [][]string
				for batchSize < len(jobKeys) {
					jobKeys, batches = jobKeys[batchSize:], append(batches, jobKeys[0:batchSize:batchSize])
				}
				batches = append(batches, jobKeys)
				for _, batch := range batches {
					sendEvents(batch)
				}
				// create new slice to allow gc previous big one
				jobKeys = make([]string, 0, batchSize)
			} else if len(jobKeys) >= batchSize {
				jobKeys = utils.MakeUnique(jobKeys)
				sendEvents(jobKeys)
				jobKeys = jobKeys[:0]
			}
		case <-ticker.C:
			if len(jobKeys) == 0 {
				continue
			}
			jobKeys = utils.MakeUnique(jobKeys)
			sendEvents(jobKeys)
			jobKeys = jobKeys[:0]
		case <-closeCh:
			return
		}
	}
}

func (cl *ClusterHandler) assignJobs(keys []string) {
	if !cl.cluster.IsLeader() {
		return
	}
	if keys == nil {
		return
	}
	cl.assignJobsChLock.RLock()
	cl.assignJobsCh <- keys
	cl.assignJobsChLock.RUnlock()
}

func (cl *ClusterHandler) handleExecutedJobs(executedJobsCh <-chan cluster.JobExecuted) {
	for payload := range executedJobsCh {
		cmd := cluster.PrepareJobExecutedCommand(payload.JobKey, payload.Error, payload.ExecutedTime)
		_, _ = cl.cluster.SyncApplyHelper(cmd, "JobExecutedCommand")
	}
}

func NewClusterHandler(typeProvider provider.TypeProvider, executor executor) *ClusterHandler {
	return &ClusterHandler{
		typeProvider: typeProvider,
		executor:     executor,
		nextPeer:     utils.NewRoundRobinStrings(make([]string, 0)),
	}
}
