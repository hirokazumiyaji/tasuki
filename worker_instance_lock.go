package tasuki

import "sync"

func (w *Worker) instanceMutex(instanceID string) *sync.Mutex {
	w.instMu.Lock()
	defer w.instMu.Unlock()
	if m, ok := w.instLock[instanceID]; ok {
		return m
	}
	m := &sync.Mutex{}
	w.instLock[instanceID] = m
	return m
}
