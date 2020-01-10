package jobs

import (
	"errors"
	jsoniter "github.com/json-iterator/go"
	"time"
)

var json = jsoniter.ConfigFastest

var (
	ErrPastTime = errors.New("time before now")
)

var _ Job = (*OneTimeJob)(nil)

type OneTimeJob struct {
	BaseJob
	Data        interface{}
	TriggerTime time.Time
	Executed    bool
}

func (j *OneTimeJob) NextTriggerTime() time.Time {
	t := time.Time{}
	if !j.Executed {
		t = j.TriggerTime
	}
	return t
}

func (j *OneTimeJob) PostExecution(_ time.Time, err error) {
	j.Executed = true
}

func (j *OneTimeJob) Marshal() ([]byte, error) {
	return json.Marshal(j)
}

func (j *OneTimeJob) Unmarshal(b []byte) error {
	return json.Unmarshal(b, j)
}

// data is optional and may be nil
func NewOneTimeJob(jType, key string, triggerTime time.Time, data interface{}) (*OneTimeJob, error) {
	baseJob, err := NewBaseJob(jType, key)
	if err != nil {
		return nil, err
	}

	if triggerTime.Before(time.Now()) {
		return nil, ErrPastTime
	}

	job := &OneTimeJob{
		BaseJob:     baseJob,
		Data:        data,
		TriggerTime: triggerTime,
	}
	return job, nil
}
