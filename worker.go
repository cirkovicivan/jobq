package main

import "fmt"

type Worker struct {
	queue   *Queue
	process func(Job) error
}

func NewWorker(queue *Queue, process func(Job) error) *Worker {
	return &Worker{queue: queue, process: process}
}

func (w *Worker) Start() {
	for {
		job, ok := w.queue.Dequeue()

		if !ok {
			// Shutdown worker
			return
		}

		err := w.process(job)

		if err != nil {
			fmt.Printf("job %d failed: %v\n", job.ID, err)
			continue
		}

		fmt.Printf("job %d succeeded\n", job.ID)
	}
}
