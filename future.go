package gds

import (
	"github.com/integration-system/gds/jobs"
	"github.com/integration-system/gds/utils"
	"time"
)

type future struct {
	timer    *time.Timer
	job      jobs.Job
	running  *utils.AtomicBool
	canceled *utils.AtomicBool
}

func (f *future) Cancel() {
	f.canceled.Set(true)
	f.timer.Stop()
}

func (f *future) IsRunning() bool {
	return f.running.Get()
}

func (f *future) IsCanceled() bool {
	return f.canceled.Get()
}

func (f *future) Run() {
	f.running.Set(true)
}
