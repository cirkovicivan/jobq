package main

import "sync"

type Queue struct {
	mu   sync.Mutex
	jobs []Job
}

func (q *Queue) Enqueue(job Job) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.jobs = append(q.jobs, job)
}

func (q *Queue) Dequeue() (Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.jobs) == 0 {
		return Job{}, false
	}

	job := q.jobs[0]
	q.jobs = q.jobs[1:]

	return job, true
}
