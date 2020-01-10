package utils

import (
	"sync"
)

type RoundRobinStrings struct {
	lock    *sync.Mutex
	strings []string
	index   int
}

func (u *RoundRobinStrings) Get() string {
	u.lock.Lock()
	u.index = (u.index + 1) % len(u.strings)
	addr := u.strings[u.index]
	u.lock.Unlock()
	return addr
}

func (u *RoundRobinStrings) Update(strings []string) {
	u.lock.Lock()
	u.index = -1
	u.strings = strings
	u.lock.Unlock()
}

func NewRoundRobinStrings(strings []string) *RoundRobinStrings {
	return &RoundRobinStrings{
		strings: strings,
		index:   -1,
		lock:    new(sync.Mutex),
	}
}
