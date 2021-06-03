// Copyright 2021 Vitaliy Filippov

package internal

import (
	"sync"
	"sync/atomic"
)

type TryMutex struct {
	mutex sync.Mutex
	locked int32
}

func (m *TryMutex) Lock() {
	m.mutex.Lock()
	atomic.StoreInt32(&m.locked, 1)
}

func (m *TryMutex) Unlock() {
	m.mutex.Unlock()
	atomic.StoreInt32(&m.locked, 0)
}

func (m *TryMutex) TryLock() bool {
	if atomic.CompareAndSwapInt32(&m.locked, 0, 1) {
		m.mutex.Lock()
		return true
	}
	return false
}
