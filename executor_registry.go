package gds

import (
	"github.com/integration-system/gds/jobs"
	"sync"
)

type JobExecutor func(jobs.Job) error

type executorRegistry interface {
	Register(jType string, executor JobExecutor)
	Unregister(jType string)
	GetExecutor(jType string) (JobExecutor, bool)
}

type defaultExecutorRegistry struct {
	lock     sync.RWMutex
	registry map[string]JobExecutor
}

func (r *defaultExecutorRegistry) Register(jType string, executor JobExecutor) {
	r.lock.Lock()
	r.registry[jType] = executor
	r.lock.Unlock()
}

func (r *defaultExecutorRegistry) Unregister(jType string) {
	r.lock.Lock()
	delete(r.registry, jType)
	r.lock.Unlock()
}

func (r *defaultExecutorRegistry) GetExecutor(jType string) (JobExecutor, bool) {
	r.lock.RLock()
	e, ok := r.registry[jType]
	r.lock.RUnlock()
	return e, ok
}

func newDefaultExecutorRegistry() executorRegistry {
	return &defaultExecutorRegistry{
		registry: make(map[string]JobExecutor),
	}
}
