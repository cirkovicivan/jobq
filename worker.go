package main

import "fmt"

type Worker struct {
	queue *Queue
}

func NewWorker(queue *Queue) *Worker {
	w := &Worker{queue: queue}
	return w
}

func (w *Worker) Start() {
	for {
		job, ok := w.queue.Dequeue()

		if !ok {

		}

		fmt.Printf("%d", job.ID)
	}
}
