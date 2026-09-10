// Package ctxlock provides cancellation-aware serialization for backend operations.
package ctxlock

import (
	"context"
	"sync"
)

// Mutex is a context-aware mutex. Its zero value is ready for use. Do not copy
// a Mutex after first use. Cancellation affects waiting, not the current holder.
type Mutex struct {
	once sync.Once
	held chan struct{}
}

// Lock waits for exclusive access or context cancellation. Even an uncontended
// lock rejects an already canceled context before the caller can start an operation.
func (m *Mutex) Lock(ctx context.Context) error {
	m.once.Do(func() { m.held = make(chan struct{}, 1) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.held <- struct{}{}:
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	}
}

// Unlock releases a successfully acquired lock.
func (m *Mutex) Unlock() { <-m.held }
