package main

import "sync"

type Queue struct {
	mu     *sync.Mutex
	jobs   []Job
	cond   *sync.Cond
	closed bool
}

func NewQueue() *Queue {
	mu := sync.Mutex{}

	return &Queue{mu: &mu, jobs: []Job{}, cond: sync.NewCond(&mu)}
}

func (q *Queue) Enqueue(job Job) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.jobs = append(q.jobs, job)

	q.cond.Signal()
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
	q.jobs = q.jobs[1:]

	return job, true
}

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.closed = true
	q.cond.Broadcast()
}
