package jobs

import (
	"errors"
	"time"
)

var (
	ErrEmptyJobKey  = errors.New("empty job key")
	ErrEmptyJobType = errors.New("empty job type")
)

type State uint32

const (
	_ State = iota
	StateScheduled
	StateUnacquired
	StateExhausted
)

func (s State) String() string {
	switch s {
	case StateScheduled:
		return "Scheduled"
	case StateUnacquired:
		return "Unacquired"
	case StateExhausted:
		return "Exhausted"
	default:
		return "Unknown"
	}
}

// Marshaler is the interface implemented by types that
// can marshal themselves.
type Marshaler interface {
	Marshal() ([]byte, error)
}

// Unmarshaler is the interface implemented by types
// that can unmarshal a byte description of themselves.
type Unmarshaler interface {
	Unmarshal([]byte) error
}

type Job interface {
	Key() string
	Type() string

	// Should return Time.IsZero() when job is done.
	NextTriggerTime() time.Time
	// Used to count executions and post-processing.
	PostExecution(time.Time, error)

	Marshaler
	Unmarshaler
}

type BaseJob struct {
	JKey  string
	JType string
}

func (j *BaseJob) Key() string {
	return j.JKey
}

func (j *BaseJob) Type() string {
	return j.JType
}

func NewBaseJob(jType, key string) (BaseJob, error) {
	if key == "" {
		return BaseJob{}, ErrEmptyJobKey
	}
	if jType == "" {
		return BaseJob{}, ErrEmptyJobType
	}
	return BaseJob{
		JKey:  key,
		JType: jType,
	}, nil
}
