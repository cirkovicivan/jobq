package jobq

import (
	"errors"
	"fmt"
)

type Worker struct {
	queue   *Queue
	process func(Job) error
}

func NewWorker(queue *Queue, process func(Job) error) (*Worker, error) {
	if queue == nil {
		return nil, errors.New("queue cannot be nil")
	}

	if process == nil {
		return nil, errors.New("process function cannot be nil")
	}

	return &Worker{
		queue:   queue,
		process: process,
	}, nil
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

		// fmt.Printf("job %d succeeded\n", job.ID)
	}
}
