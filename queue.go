package queue

func NewQueue(total int) *Queue {
	ch := make(chan int, total)
	for i := 0; i < total; i++ {
		ch <- i
	}

	q := &Queue{
		i:           ch,
		pop:         make(chan int),
		emptyNotify: make(chan struct{}),
	}
	go q.background()
	return q
}

type Queue struct {
	i           chan int
	pop         chan int
	emptyNotify chan struct{}
}

func (q *Queue) background() {
	for {
		q.pop <- <-q.i
		if len(q.i) == 0 {
			q.emptyNotify <- struct{}{}
		}
	}
}

func (q *Queue) Pop() <-chan int {
	return q.pop
}

func (q *Queue) Empty() <-chan struct{} {
	return q.emptyNotify
}

func (q *Queue) Close() {
	close(q.emptyNotify)
}
