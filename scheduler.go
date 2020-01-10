package gds

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/integration-system/gds/cluster"
	"github.com/integration-system/gds/config"
	"github.com/integration-system/gds/jobs"
	"github.com/integration-system/gds/provider"
	"github.com/integration-system/gds/store"
	"github.com/integration-system/gds/utils"
	log "github.com/integration-system/isp-log"
	"time"
)

var (
	ErrJobAlreadyDone = errors.New("job already done")
)

type Scheduler interface {
	WaitCluster(context.Context) bool
	Shutdown(ctx context.Context) error
	RegisterExecutor(jobType string, executor JobExecutor, newJobFunc provider.NewJobFunc) Scheduler
	UnregisterExecutor(jobType string)

	ScheduleJob(job jobs.Job) error
	GetJob(key string) (*store.JobInfo, error)
	DeleteJob(key string) error
	GetAllJobs() []store.JobInfo
	GetJobsByType(jobType string) []store.JobInfo
}

type scheduler struct {
	store        *store.Store
	registry     executorRegistry
	executor     executor
	typeProvider provider.TypeProvider
	raftAdapter  *RaftAdapter
}

func (s *scheduler) ScheduleJob(job jobs.Job) error {
	var err error
	s.store.VisitReadonlyState(func(state store.ReadonlyState) {
		_, err = state.GetJob(job.Key())
	})

	if err == nil {
		return store.ErrJobAlreadyExists
	}

	b, err := job.Marshal()
	if err != nil {
		return err
	}
	cmd := cluster.PrepareInsertJobCommand(job.Type(), b)
	_, err = s.raftAdapter.ClusterClient.SyncApplyHelper(cmd, "InsertJobCommand")
	return err
}

func (s *scheduler) GetJob(key string) (*store.JobInfo, error) {
	var (
		job *store.JobInfo
		err error
	)
	s.store.VisitReadonlyState(func(state store.ReadonlyState) {
		job, err = state.GetJob(key)
	})
	return job, err
}

func (s *scheduler) DeleteJob(key string) error {
	var (
		err    error
		oldJob *store.JobInfo
	)
	s.store.VisitReadonlyState(func(state store.ReadonlyState) {
		oldJob, err = state.GetJob(key)
	})

	if err != nil {
		return err
	}
	if oldJob.State == jobs.StateExhausted {
		return ErrJobAlreadyDone
	}

	cmd := cluster.PrepareDeleteJobCommand(key)
	_, err = s.raftAdapter.ClusterClient.SyncApplyHelper(cmd, "DeleteJobCommand")
	return err
}

func (s *scheduler) GetAllJobs() []store.JobInfo {
	var result []store.JobInfo
	s.store.VisitReadonlyState(func(state store.ReadonlyState) {
		result = state.GetAllJobs()
	})
	return result
}

func (s *scheduler) GetJobsByType(jobType string) []store.JobInfo {
	var result []store.JobInfo
	s.store.VisitReadonlyState(func(state store.ReadonlyState) {
		result = state.GetJobsByType(jobType)
	})
	return result
}

func (s *scheduler) RegisterExecutor(jType string, executor JobExecutor, newJobFunc provider.NewJobFunc) Scheduler {
	s.registry.Register(jType, executor)
	s.typeProvider.RegisterJobProvider(jType, newJobFunc)
	return s
}

func (s *scheduler) UnregisterExecutor(jType string) {
	s.registry.Unregister(jType)
	s.typeProvider.UnregisterJobProvider(jType)
}

func (s *scheduler) WaitCluster(ctx context.Context) bool {
	return utils.Wait(ctx, func() bool {
		return s.raftAdapter.ClusterClient.Leader() != ""
	}, 60*time.Millisecond)
}

func (s *scheduler) Shutdown(ctx context.Context) error {
	errs := new(multierror.Error)

	if err := s.raftAdapter.Shutdown(ctx); err != nil {
		err = fmt.Errorf("shutdown raft: %v", err)
		errs = multierror.Append(errs, err)
	}
	if err := s.executor.Shutdown(ctx); err != nil {
		errs = multierror.Append(errs, err)
	}
	return errs.ErrorOrNil()
}

func NewScheduler(config config.ClusterConfiguration) (Scheduler, error) {
	// TODO remove
	_ = log.SetLevel("fatal")
	//_ = log.SetLevel("debug")
	executedJobsCh := make(chan cluster.JobExecuted, 100)

	typeProvider := provider.NewTypeProvider()
	executorRegistry := newDefaultExecutorRegistry()
	executor := newDefaultRuntimeExecutor(executorRegistry, executedJobsCh, config.JobExecutionTimeout)

	clusterHandler := NewClusterHandler(typeProvider, executor)

	raftAdapter, err := NewRaftAdapter(config, clusterHandler, typeProvider)
	if err != nil {
		return nil, err
	}

	s := &scheduler{
		registry:     executorRegistry,
		executor:     executor,
		typeProvider: typeProvider,
		raftAdapter:  raftAdapter,
		store:        raftAdapter.RaftStore,
	}

	clusterHandler.cluster = raftAdapter.ClusterClient
	go clusterHandler.listenLeaderCh(raftAdapter.RaftStore)
	go clusterHandler.handleExecutedJobs(executedJobsCh)

	return s, nil
}
