package queue

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// work is int32, not int, so its increment can go through sync/atomic:
// Manager runs multiple Do() goroutines against the same Worker concurrently.
type work int32

func (w *work) Do(v interface{}) interface{} {
	fmt.Printf("-->%#v\n%#v\n", w, v)
	atomic.AddInt32((*int32)(w), 1)
	return nil
}

func TestQueuManager(t *testing.T) {
	s := mySimpler{i: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}

	var w work
	ctx := context.Background()
	m := NewManager(ctx, &w, s)

	go m.Do()
	go m.Do()
	go m.Do()
	go m.Do()
	go m.Do()
	go m.Do()

	<-m.End()

	if w != 10 {
		t.Error("not finish", w)
	}
}

type slow int32

func (w *slow) Do(v interface{}) interface{} {
	time.Sleep(500 * time.Millisecond)
	atomic.AddInt32((*int32)(w), 1)
	fmt.Println(w, v)
	return nil
}

func TestQueuManagerTimeout(t *testing.T) {
	// items := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	s := mySimpler{i: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}

	var w slow
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	m := NewManager(ctx, &w, s)

	go m.Do()
	go m.Do()
	go m.Do()
	go m.Do()

	<-m.End()

	if w == 10 {
		t.Error("not timeout", w)
	}
}

type workPanic int

func (w *workPanic) Do(v interface{}) interface{} {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
		}
	}()
	panic(errors.New("fail"))
	return nil
}

func TestQueuManagerWhenWorkerHasPanic(t *testing.T) {
	s := mySimpler{i: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}

	var w workPanic
	ctx := context.Background()
	m := NewManager(ctx, &w, s)

	go m.Do()
	go m.Do()

	<-m.End()

	if w != 0 {
		t.Error("not finish", w)
	}
}

type workError int

func (w *workError) Do(v interface{}) interface{} {
	if (v.(int) % 2) == 0 {
		return errors.New("it's error")
	}
	return nil
}

func TestQueuManagerWhenWorkerHasError(t *testing.T) {
	s := mySimpler{i: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}

	var w workError
	ctx := context.Background()
	m := NewManager(ctx, &w, s)

	go m.Do()
	go m.Do()

	<-m.End()

	if w != 0 {
		t.Error("not finish", w)
	}

	for err := range m.Response() {
		fmt.Println("---->", err)
	}
}

func TestQueuManagerResponseClosesWhenContextCancelledBeforeAllItemsProcessed(t *testing.T) {
	items := make([]int, 30)
	s := mySimpler{i: items}

	var w work
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before Do() ever starts, so every item should be skipped

	m := NewManager(ctx, &w, s)
	go m.Do()

	select {
	case <-m.End():
	case <-time.After(2 * time.Second):
		t.Fatal("Manager did not end after context cancellation")
	}

	done := make(chan struct{})
	go func() {
		for range m.Response() {
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Response channel was never closed after context cancellation")
	}
}

func TestQueuManagerCountsEveryItemUnderConcurrentWorkers(t *testing.T) {
	const total = 200
	s := mySimpler{i: make([]int, total)}

	var w work
	ctx := context.Background()
	m := NewManager(ctx, &w, s)

	for i := 0; i < 20; i++ {
		go m.Do()
	}

	<-m.End()

	if int(w) != total {
		t.Errorf("expected %d, got %d (lost increments under concurrent Do calls)", total, w)
	}
}
