package main

import (
	"errors"
	"testing"
)

func TestWorkerProcessJob(t *testing.T) {
	q := NewQueue()

	j := Job{ID: 1, Name: "Job1"}

	q.Enqueue(j)

	processedJobID := 0

	w := NewWorker(q, func(j Job) error {
		processedJobID = j.ID
		return nil
	})

	w.Start()

	if processedJobID != 1 {
		t.Fatalf("Expected processedJob to have id 1 instead of %d", processedJobID)
	}
}

func TestWorkerProcessesMultipleJobs(t *testing.T) {
	q := NewQueue()

	job1 := Job{ID: 1, Name: "first"}
	job2 := Job{ID: 2, Name: "second"}
	job3 := Job{ID: 3, Name: "third"}

	q.Enqueue(job1)
	q.Enqueue(job2)
	q.Enqueue(job3)

	processedJobIDs := []int{}

	w := NewWorker(q, func(j Job) error {
		processedJobIDs = append(processedJobIDs, j.ID)
		return nil
	})

	w.Start()

	for i := 0; i < len(processedJobIDs); i++ {
		if processedJobIDs[i] != i+1 {
			t.Fatalf("Expected processedJob to have id %d instead of %d", i+1, processedJobIDs[i])
		}
	}

}

func TestWorkerContinuesAfterError(t *testing.T) {
	q := NewQueue()

	job1 := Job{ID: 1, Name: "first"}
	job2 := Job{ID: 2, Name: "second"}
	job3 := Job{ID: 3, Name: "third"}

	q.Enqueue(job1)
	q.Enqueue(job2)
	q.Enqueue(job3)

	attemptedJobIDs := []int{}

	w := NewWorker(q, func(j Job) error {
		attemptedJobIDs = append(attemptedJobIDs, j.ID)
		if j.ID == 2 {
			return errors.New("job failed")
		}
		return nil
	})

	w.Start()

	for i := 0; i < len(attemptedJobIDs); i++ {
		if attemptedJobIDs[i] != i+1 {
			t.Fatalf("Expected processedJob to have id %d instead of %d", i+1, attemptedJobIDs[i])
		}
	}
}

func TestWorkerStopsAfterQueueClosed(t *testing.T) {
	q := NewQueue()
	q.closed = true

	attemptedJob := 0

	w := NewWorker(q, func(j Job) error {
		attemptedJob = j.ID
		return nil
	})

	w.Start()

	if attemptedJob != 0 {
		t.Fatalf("Expected attemptedJob to be 0 instead of %d", attemptedJob)
	}
}
