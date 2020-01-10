package utils

import "sync/atomic"

type AtomicBool struct {
	v int32
}

func (b *AtomicBool) Get() bool {
	return atomic.LoadInt32(&b.v) == 1
}

func (b *AtomicBool) Set(value bool) {
	if value {
		atomic.StoreInt32(&b.v, 1)
	} else {
		atomic.StoreInt32(&b.v, 0)
	}
}

func NewAtomicBool(init bool) *AtomicBool {
	b := &AtomicBool{}
	if init {
		b.v = 1
	}
	return b
}
