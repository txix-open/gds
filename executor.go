package gds

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/integration-system/gds/cluster"
	"github.com/integration-system/gds/jobs"
	"github.com/integration-system/gds/utils"
	"sync"
	"time"
)

const (
	DefaultJobExecutionTimeout = 5 * time.Second
)

var (
	ErrJobTimeoutExceeded = errors.New("job execution timeout exceeded")
)

type executor interface {
	Shutdown(ctx context.Context) error
	AddJob(job jobs.Job)
	CancelJob(key string) bool
}

type defaultRuntimeExecutor struct {
	registry         executorRegistry
	runningFutures   sync.WaitGroup
	lock             sync.Mutex
	fMap             map[string]*future
	executedJobsCh   chan<- cluster.JobExecuted
	executionTimeout time.Duration

	logger hclog.Logger
}

func (e *defaultRuntimeExecutor) CancelJob(key string) bool {
	e.lock.Lock()
	defer e.lock.Unlock()

	canceled := false
	f, ok := e.fMap[key]
	if ok {
		f.Cancel()
		delete(e.fMap, key)
		canceled = true
	}

	return canceled
}

func (e *defaultRuntimeExecutor) Shutdown(ctx context.Context) error {
	// try release not running triggers
	e.lock.Lock()
	for key, f := range e.fMap {
		if !f.IsRunning() {
			f.Cancel()
			delete(e.fMap, key)
		}
	}
	e.lock.Unlock()

	// await all running triggers
	awaitRunning := make(chan struct{}, 1)
	go func() {
		e.runningFutures.Wait()
		select {
		case <-ctx.Done():
		case awaitRunning <- struct{}{}:
		}
	}()
	select {
	case <-ctx.Done():
	case <-awaitRunning:
		close(e.executedJobsCh)
	}

	return nil
}

func (e *defaultRuntimeExecutor) AddJob(job jobs.Job) {
	future := &future{
		job:      job,
		running:  utils.NewAtomicBool(false),
		canceled: utils.NewAtomicBool(false),
	}
	f := e.makeF(future)

	var dur time.Duration
	now := time.Now()
	nextTime := job.NextTriggerTime()
	if now.After(nextTime) {
		dur = 0
	} else {
		dur = nextTime.Sub(now)
	}

	future.timer = time.AfterFunc(dur, f)

	e.lock.Lock()
	e.fMap[job.Key()] = future
	e.lock.Unlock()
}

func (e *defaultRuntimeExecutor) makeF(f *future) func() {
	return func() {
		if f.IsCanceled() {
			return
		}

		now := time.Now()
		f.Run()
		e.runningFutures.Add(1)
		defer func() {
			e.lock.Lock()
			delete(e.fMap, f.job.Key())
			e.lock.Unlock()

			f.running.Set(false)

			e.runningFutures.Done()
		}()

		exec, ok := e.registry.GetExecutor(f.job.Type())
		if !ok {
			e.logger.Error(fmt.Sprintf("defaultRuntimeExecutor: not found executor for job type: %v", f.job.Type()))
			return
		}

		doneChan := make(chan error, 1)
		go func() {
			defer func() {
				if err := recover(); err != nil {
					doneChan <- fmt.Errorf("%v", err)
				}
			}()
			doneChan <- exec(f.job)
		}()

		var err error
		if e.executionTimeout > 0 {
			timeout := time.NewTimer(e.executionTimeout)
			defer timeout.Stop()

			select {
			case <-timeout.C:
				err = ErrJobTimeoutExceeded
			case err = <-doneChan:
			}
		} else {
			err = <-doneChan
		}

		var errStr string
		if err != nil {
			errStr = err.Error()
			// TODO remove log
			e.logger.Warn(fmt.Sprintf("defaultRuntimeExecutor: job %s type %s has finished with err '%v'", f.job.Key(), f.job.Type(), err))
		}

		executedJob := cluster.JobExecuted{
			JobKey:       f.job.Key(),
			Error:        errStr,
			ExecutedTime: now,
		}

		e.executedJobsCh <- executedJob
	}
}

func newDefaultRuntimeExecutor(registry executorRegistry, executedJobsCh chan<- cluster.JobExecuted, executionTimeout time.Duration, logger hclog.Logger) executor {
	if executionTimeout == 0 {
		executionTimeout = DefaultJobExecutionTimeout
	}
	return &defaultRuntimeExecutor{
		registry:         registry,
		fMap:             make(map[string]*future),
		executedJobsCh:   executedJobsCh,
		executionTimeout: executionTimeout,
		logger:           logger,
	}
}
