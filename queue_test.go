package main

import (
	"fmt"
	"sync"
	"testing"
)

func TestEnqueue(t *testing.T) {
	queue := Queue{}

	job := Job{
		ID:   1,
		Name: "test-job",
	}

	queue.Enqueue(job)

	if len(queue.jobs) != 1 {
		t.Fatalf("Expected queue length 1, got %d", len(queue.jobs))
	}
}

func TestDequeue(t *testing.T) {
	queue := Queue{}

	job1 := Job{ID: 1, Name: "first"}
	job2 := Job{ID: 2, Name: "second"}
	job3 := Job{ID: 3, Name: "third"}

	queue.Enqueue(job1)
	queue.Enqueue(job2)
	queue.Enqueue(job3)

	got, ok := queue.Dequeue()

	if !ok {
		t.Fatalf("Expected a job, got empty queue")
	}

	if got.ID != 1 {
		t.Fatalf("Expected job 1 got %d", got.ID)
	}
}

func TestDequeueEmpty(t *testing.T) {
	queue := Queue{}

	_, ok := queue.Dequeue()

	if ok {
		t.Fatal("expected dequeue to fail on empty queue")
	}
}
func TestConcurentWorkers(t *testing.T) {
	queue := Queue{}

	// create jobs
	for i := range 100 {
		job := Job{
			ID:   i,
			Name: fmt.Sprintf("Job %d", i),
		}
		queue.Enqueue(job)
	}

	var wg sync.WaitGroup
	var resultsMu sync.Mutex
	results := make(map[int]bool)

	for range 100 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			got, ok := queue.Dequeue()
			if !ok {
				t.Error("expected a job, got empty queue")
				return
			}

			resultsMu.Lock()
			results[got.ID] = true
			resultsMu.Unlock()
		}()
	}

	wg.Wait()

	if len(results) != 100 {
		t.Fatalf("expected 100 unique jobs, got %d", len(results))
	}

	for i := range 100 {
		if !results[i] {
			t.Fatalf("job %d was not processed", i)
		}
	}
}
