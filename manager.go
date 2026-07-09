package queue

import (
	"context"
)

// Worker is the represent of your business logic and return anything you want,
// such as error or string or any type
type Worker interface {
	Do(v interface{}) interface{}
}

type Manager struct {
	count  chan struct{}
	notify chan struct{}
	total  int
	w      Worker
	ctx    context.Context
	q      *Queue
	chv    chan interface{}
}

func NewManager(ctx context.Context, w Worker, s Simpler) *Manager {
	q := NewQueue(s)
	total := s.Len()
	m := &Manager{
		count:  make(chan struct{}, total),
		notify: make(chan struct{}),
		total:  total,
		w:      w,
		ctx:    ctx,
		q:      q,
		chv:    make(chan interface{}, total),
	}
	go m.counting(q.Empty())
	return m
}

func (m *Manager) Do() {
	for x := range m.q.Pop() {
		select {
		case <-m.ctx.Done():
			// Drop the item instead of processing it, but still count it so
			// counting() reaches total and closes chv/sends notify exactly once.
		default:
			m.chv <- m.w.Do(x)
		}
		m.count <- struct{}{}
	}
}

func (m *Manager) counting(x <-chan struct{}) {
	<-x
	for i := 0; i < m.total; i++ {
		<-m.count
	}
	close(m.chv)
	m.notify <- struct{}{}
}

func (m *Manager) End() <-chan struct{} {
	return m.notify
}

func (m *Manager) Response() <-chan interface{} {
	return m.chv
}
