package jobq

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWorkerProcessJob(t *testing.T) {
	q := NewQueue()

	j := Job{ID: 1, Name: "Job1"}

	q.Enqueue(j)

	processedJobID := 0

	w, err := NewWorker(q, func(j Job) error {
		processedJobID = j.ID
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error creating worker: %v", err)
	}

	q.Close()
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

	w, err := NewWorker(q, func(j Job) error {
		processedJobIDs = append(processedJobIDs, j.ID)
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error creating worker: %v", err)
	}

	q.Close()
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

	w, err := NewWorker(q, func(j Job) error {
		attemptedJobIDs = append(attemptedJobIDs, j.ID)
		if j.ID == 2 {
			return errors.New("job failed")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error creating worker: %v", err)
	}

	q.Close()
	w.Start()

	for i := 0; i < len(attemptedJobIDs); i++ {
		if attemptedJobIDs[i] != i+1 {
			t.Fatalf("Expected processedJob to have id %d instead of %d", i+1, attemptedJobIDs[i])
		}
	}
}

func TestWorkerStopsAfterQueueClosed(t *testing.T) {
	q := NewQueue()

	attemptedJob := 0

	w, err := NewWorker(q, func(j Job) error {
		attemptedJob = j.ID
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error creating worker: %v", err)
	}

	q.Close()
	w.Start()

	if attemptedJob != 0 {
		t.Fatalf("Expected attemptedJob to be 0 instead of %d", attemptedJob)
	}
}

func TestMultipleWorkers(t *testing.T) {
	q := NewQueue()

	const (
		jobCount    = 1000
		workerCount = 16
	)

	for i := range jobCount {
		q.Enqueue(Job{ID: i + 1, Name: fmt.Sprintf("Job %d", i+1)})
	}

	var wg sync.WaitGroup
	var resultsMu sync.Mutex

	results := make(map[int]int)
	workerResults := make(map[int]int)

	for workerID := range workerCount {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			w, err := NewWorker(q, func(j Job) error {
				time.Sleep(1 * time.Millisecond)

				resultsMu.Lock()
				results[j.ID]++
				workerResults[id]++
				resultsMu.Unlock()

				return nil
			})

			if err != nil {
				t.Fatalf("unexpected error creating worker: %v", err)
			}

			w.Start()
		}(workerID)
	}

	q.Close()

	wg.Wait()

	for id := 1; id <= jobCount; id++ {
		if results[id] != 1 {
			t.Fatalf("job %d was processed %d times", id, results[id])
		}
	}

	workersUsed := 0

	for _, jobsProcessed := range workerResults {
		if jobsProcessed > 0 {
			workersUsed++
		}
	}

	if workersUsed < 2 {
		t.Fatalf("expected jobs to be distributed between multiple workers, but only %d worker processed jobs", workersUsed)
	}
}

func TestNewWorkerRejectsNilHandler(t *testing.T) {
	q := NewQueue()

	_, err := NewWorker(q, nil)

	if err == nil {
		t.Fatal("expected error when creating worker with nil handler")
	}
}
