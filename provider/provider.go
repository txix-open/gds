package provider

import (
	"github.com/integration-system/gds/jobs"
)

type NewJobFunc func() jobs.Job

type TypeProvider interface {
	RegisterJobProvider(name string, newJob NewJobFunc)
	UnregisterJobProvider(name string)
	Get(name string) NewJobFunc
}

type typeProvider struct {
	jobs map[string]NewJobFunc
}

func (tp *typeProvider) RegisterJobProvider(name string, newJob NewJobFunc) {
	tp.jobs[name] = newJob
}

func (tp *typeProvider) UnregisterJobProvider(name string) {
	delete(tp.jobs, name)
}

func (tp *typeProvider) Get(name string) NewJobFunc {
	return tp.jobs[name]
}

func NewTypeProvider() TypeProvider {
	return &typeProvider{
		jobs: make(map[string]NewJobFunc),
	}
}
