package jobs

import (
	"fmt"
	"github.com/robfig/cron/v3"
	"time"
)

var (
	ErrInvalidCronSpec = "invalid cron spec: %v"
)

var _ Job = (*CronJob)(nil)

type CronJob struct {
	BaseJob
	Data     interface{}
	CronSpec string
	schedule cron.Schedule
}

func (j *CronJob) NextTriggerTime() time.Time {
	return j.schedule.Next(time.Now())
}

func (j *CronJob) PostExecution(_ time.Time, _ error) {
}

func (j *CronJob) Marshal() ([]byte, error) {
	return json.Marshal(j)
}

func (j *CronJob) Unmarshal(b []byte) error {
	err := json.Unmarshal(b, j)
	if err != nil {
		return err
	}
	j.schedule, err = cron.ParseStandard(j.CronSpec)
	return err
}

// data is optional and may be nil
func NewCronJob(jType, key string, cronSpec string, data interface{}) (*CronJob, error) {
	baseJob, err := NewBaseJob(jType, key)
	if err != nil {
		return nil, err
	}

	sched, err := cron.ParseStandard(cronSpec)
	if err != nil {
		return nil, fmt.Errorf(ErrInvalidCronSpec, err)
	}

	job := &CronJob{
		BaseJob:  baseJob,
		Data:     data,
		CronSpec: cronSpec,
		schedule: sched,
	}
	return job, nil
}
