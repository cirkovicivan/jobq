package jobq

import (
	"errors"
	"sync"
)

var ErrQueueClosed = errors.New("job queue is closed")

type Queue struct {
	mu     sync.Mutex
	jobs   []Job
	cond   *sync.Cond
	closed bool
}

func NewQueue() *Queue {
	q := &Queue{}
	q.cond = sync.NewCond(&q.mu)

	return q
}

func (q *Queue) Enqueue(job Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	q.jobs = append(q.jobs, job)
	q.cond.Signal()

	return nil
}

func (q *Queue) Dequeue() (Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.jobs) == 0 && !q.closed {
		q.cond.Wait()
	}

	if len(q.jobs) == 0 && q.closed {
		return Job{}, false
	}

	job := q.jobs[0]
	q.jobs[0] = Job{}
	q.jobs = q.jobs[1:]

	return job, true
}

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.closed = true
	q.cond.Broadcast()
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.jobs)
}
