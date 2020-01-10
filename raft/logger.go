package raft

import (
	"fmt"
	"github.com/hashicorp/go-hclog"
	isplog "github.com/integration-system/isp-log"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"io"
	"log"
)

type LoggerAdapter struct {
	name string
}

func (l *LoggerAdapter) Log(level logrus.Level, msg string, args ...interface{}) {
	if l.name != "" {
		msg = fmt.Sprintf("%s:%s", l.name, msg)
	}
	if len(args) != 0 {
		metadata := make(map[string]interface{}, len(args)/2)
		for i := 0; i < len(args)-1; i += 2 {
			k := cast.ToString(args[i])
			metadata[k] = args[i+1]
		}
		isplog.WithMetadata(metadata).Log(level, 0, msg)
	} else {
		isplog.Log(level, 0, msg)
	}
}

func (l *LoggerAdapter) Trace(msg string, args ...interface{}) {
	l.Log(logrus.TraceLevel, msg, args...)
}

func (l *LoggerAdapter) Debug(msg string, args ...interface{}) {
	l.Log(logrus.DebugLevel, msg, args...)
}

func (l *LoggerAdapter) Info(msg string, args ...interface{}) {
	l.Log(logrus.InfoLevel, msg, args...)
}

func (l *LoggerAdapter) Warn(msg string, args ...interface{}) {
	l.Log(logrus.WarnLevel, msg, args...)
}

func (l *LoggerAdapter) Error(msg string, args ...interface{}) {
	l.Log(logrus.ErrorLevel, msg, args...)
}

func (l *LoggerAdapter) IsTrace() bool {
	return isplog.IsLevelEnabled(logrus.TraceLevel)
}

func (l *LoggerAdapter) IsDebug() bool {
	return isplog.IsLevelEnabled(logrus.DebugLevel)
}

func (l *LoggerAdapter) IsInfo() bool {
	return isplog.IsLevelEnabled(logrus.InfoLevel)
}

func (l *LoggerAdapter) IsWarn() bool {
	return isplog.IsLevelEnabled(logrus.WarnLevel)
}

func (l *LoggerAdapter) IsError() bool {
	return isplog.IsLevelEnabled(logrus.ErrorLevel)
}

// No need to implement that as Raft doesn't use this method.
func (l *LoggerAdapter) With(args ...interface{}) hclog.Logger {
	return l
}

func (l *LoggerAdapter) ImpliedArgs() []interface{} {
	return nil
}

func (l *LoggerAdapter) Named(name string) hclog.Logger {
	sl := *l
	if sl.name != "" {
		sl.name = sl.name + "." + name
	} else {
		sl.name = name
	}
	return &sl
}

func (l *LoggerAdapter) Name() string {
	return l.name
}

func (l *LoggerAdapter) ResetNamed(name string) hclog.Logger {
	sl := *l
	sl.name = name
	return &sl
}

// Skip
func (l *LoggerAdapter) SetLevel(level hclog.Level) {
}

func (l *LoggerAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return log.New(isplog.GetOutput(), "", 0)
}

func (l *LoggerAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return isplog.GetOutput()
}
