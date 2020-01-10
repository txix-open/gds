package store

import (
	"encoding/binary"
	json2 "encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/integration-system/gds/cluster"
	"github.com/integration-system/gds/provider"
	log "github.com/integration-system/isp-log"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"io"
	"sync"
)

var (
	json = jsoniter.ConfigFastest
)

type Store struct {
	state        *state
	lock         sync.RWMutex
	handlers     map[uint64]func(WritableState, []byte) (interface{}, error)
	typeProvider provider.TypeProvider
}

func (s *Store) Apply(l *raft.Log) interface{} {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(l.Data) < 8 {
		log.Errorf(0, "invalid log data command: %s", l.Data)
	}
	command := binary.BigEndian.Uint64(l.Data[:8])
	//log.Debugf(0, "Apply %d command. Data: %s", command, l.Data[8:])

	var (
		result interface{}
		err    error
	)
	if handler, ok := s.handlers[command]; ok {
		result, err = handler(s.state, l.Data[8:])
	} else {
		err = fmt.Errorf("unknown log command %d", command)
		log.WithMetadata(map[string]interface{}{
			"command": command,
			"body":    string(l.Data),
		}).Error(0, "unknown log command")
	}

	bytes, e := json.Marshal(result)
	if e != nil {
		panic(e) // must never occurred
	}

	logResponse := cluster.ApplyLogResponse{Result: bytes}
	if err != nil {
		logResponse.ApplyError = err.Error()
	}
	return logResponse
}

func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return newFsmSnapshot(s.state)
}

func (s *Store) Restore(rc io.ReadCloser) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	var snapshot fsmSnapshot
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return errors.WithMessage(err, "unmarshal snapshot")
	}

	newState, err := snapshot.DecodeState(s.typeProvider)
	if err != nil {
		return err
	}
	s.state = newState
	return nil
}

func (s *Store) VisitReadonlyState(f func(ReadonlyState)) {
	s.lock.RLock()
	f(s.state)
	s.lock.RUnlock()
}

type CommandsHandler interface {
	GetHandlers() map[uint64]func(WritableState, []byte) (interface{}, error)
}

func NewStore(handler CommandsHandler, typeProvider provider.TypeProvider) *Store {
	store := &Store{
		state:        newEmptyState(),
		typeProvider: typeProvider,
	}
	store.handlers = handler.GetHandlers()
	return store
}

type snapshotJobEntry struct {
	AssignedPeerID string
	JobType        string
	Job            json2.RawMessage
}

type fsmSnapshot struct {
	JobEntries  []snapshotJobEntry
	OnlinePeers []string
}

func (f *fsmSnapshot) DecodeState(typeProvider provider.TypeProvider) (*state, error) {
	state2 := &state{
		Jobs:        make(map[string]jobInfo, len(f.JobEntries)),
		OnlinePeers: f.OnlinePeers,
	}
	for _, entry := range f.JobEntries {
		jobFunc := typeProvider.Get(entry.JobType)
		job := jobFunc()
		err := job.Unmarshal(entry.Job)
		if err != nil {
			return nil, err
		}
		info := jobInfo{
			Job:            job,
			AssignedPeerID: entry.AssignedPeerID,
		}
		state2.Jobs[job.Key()] = info
	}
	return state2, nil
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		if err := json.NewEncoder(sink).Encode(f); err != nil {
			return err
		}
		return sink.Close()
	}()

	if err != nil {
		_ = sink.Cancel()
	}
	return err
}

func (f *fsmSnapshot) Release() {}

func newFsmSnapshot(state *state) (*fsmSnapshot, error) {
	jobEntries := make([]snapshotJobEntry, 0, len(state.Jobs))
	for _, info := range state.Jobs {
		job, err := info.Job.Marshal()
		if err != nil {
			return nil, err
		}
		entry := snapshotJobEntry{
			Job:            job,
			AssignedPeerID: info.AssignedPeerID,
			JobType:        info.Job.Type(),
		}
		jobEntries = append(jobEntries, entry)
	}
	return &fsmSnapshot{
		JobEntries:  jobEntries,
		OnlinePeers: state.GetPeers(),
	}, nil
}
