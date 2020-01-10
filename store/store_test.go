package store

import (
	"bytes"
	"github.com/integration-system/gds/jobs"
	"github.com/integration-system/gds/provider"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type mockSnapshotSink struct {
	bytes.Buffer
}

func (d mockSnapshotSink) Close() error {
	return nil
}

func (d mockSnapshotSink) ID() string {
	panic("implement me")
}

func (d mockSnapshotSink) Cancel() error {
	panic("implement me")
}

func TestStore_SnapshotRestore(t *testing.T) {
	a := assert.New(t)

	typeProvider := provider.NewTypeProvider()
	newJob := func() jobs.Job { return &jobs.OneTimeJob{Data: nil} }
	typeProvider.RegisterJobProvider("jtype", newJob)

	state := newEmptyState()
	state.AddPeer("peer-123")
	state.AddPeer("peer-321")
	triggerTime := time.Now().Add(time.Second).Round(time.Nanosecond)
	j, err := jobs.NewOneTimeJob("jtype", "j-1", triggerTime, nil)
	a.NoError(err)
	err = state.InsertJob(j)
	a.NoError(err)
	state.AcquireJob("j-1", "peerid1")

	store := &Store{
		state:        state,
		typeProvider: typeProvider,
	}
	snapshot, err := store.Snapshot()
	a.NoError(err)
	sink := mockSnapshotSink{}
	err = snapshot.Persist(&sink)
	a.NoError(err)

	store.state = newEmptyState()
	err = store.Restore(&sink)
	a.NoError(err)

	a.Equal(state, store.state)
}
