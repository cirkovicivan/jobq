package jobq

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestEnqueue(t *testing.T) {
	queue := NewQueue()

	job := Job{
		ID:   1,
		Name: "test-job",
	}

	queue.Enqueue(job)

	if len(queue.jobs) != 1 {
		t.Fatalf("Expected queue length 1, got %d", len(queue.jobs))
	}
}

func TestDequeueFIFO(t *testing.T) {
	queue := NewQueue()

	job1 := Job{ID: 1, Name: "first"}
	job2 := Job{ID: 2, Name: "second"}
	job3 := Job{ID: 3, Name: "third"}

	queue.Enqueue(job1)
	queue.Enqueue(job2)
	queue.Enqueue(job3)

	for i := range 3 {
		job, ok := queue.Dequeue()

		if !ok {
			t.Fatalf("Expected a job, got empty queue")
		}

		if job.ID != i+1 {
			t.Fatalf("Expected job %d got %d", i+1, job.ID)
		}
	}
}

func TestConcurrentDequeue(t *testing.T) {
	queue := NewQueue()

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

func TestCloseIsIdempotent(t *testing.T) {
	queue := NewQueue()

	queue.Close()
	queue.Close()
	queue.Close()

	if _, ok := queue.Dequeue(); ok {
		t.Fatal("expected queue to be closed and empty")
	}
}

func TestCloseDrainsQueuedJobs(t *testing.T) {
	queue := NewQueue()

	jobs := []Job{
		{ID: 1},
		{ID: 2},
		{ID: 3},
	}

	for _, job := range jobs {
		if err := queue.Enqueue(job); err != nil {
			t.Fatalf("unexpected enqueue error: %v", err)
		}
	}

	queue.Close()

	for _, expected := range jobs {
		job, ok := queue.Dequeue()

		if !ok {
			t.Fatal("expected queued job after close")
		}

		if job.ID != expected.ID {
			t.Fatalf("expected job %d, got %d", expected.ID, job.ID)
		}
	}

	_, ok := queue.Dequeue()

	if ok {
		t.Fatal("expected dequeue to return false after queue was drained")
	}
}

func TestLen(t *testing.T) {
	queue := NewQueue()

	if got := queue.Len(); got != 0 {
		t.Fatalf("expected length 0, got %d", got)
	}

	queue.Enqueue(Job{ID: 1})
	queue.Enqueue(Job{ID: 2})
	queue.Enqueue(Job{ID: 3})

	if got := queue.Len(); got != 3 {
		t.Fatalf("expected length 3, got %d", got)
	}

	queue.Dequeue()

	if got := queue.Len(); got != 2 {
		t.Fatalf("expected length 2, got %d", got)
	}
}

func TestDequeueBlocksWhenQueueIsEmpty(t *testing.T) {
	queue := NewQueue()

	done := make(chan struct{})

	go func() {
		queue.Dequeue()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Dequeue returned before a job was available")
	default:
	}

	queue.Enqueue(Job{ID: 1})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not return after a job was enqueued")
	}
}

func TestCloseWakesBlockedDequeue(t *testing.T) {
	queue := NewQueue()

	done := make(chan struct{})

	go func() {
		_, ok := queue.Dequeue()

		if ok {
			t.Error("expected Dequeue to return false after Close")
		}

		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Dequeue returned before Close")
	case <-time.After(50 * time.Millisecond):
	}

	queue.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not return after Close")
	}
}

func TestEnqueueAfterClose(t *testing.T) {
	queue := NewQueue()

	queue.Close()

	err := queue.Enqueue(Job{ID: 1})

	if err != ErrQueueClosed {
		t.Fatalf("expected ErrQueueClosed, got %v", err)
	}

	if got := queue.Len(); got != 0 {
		t.Fatalf("expected queue length 0, got %d", got)
	}
}

func TestConcurrentProducersAndConsumers(t *testing.T) {
	queue := NewQueue()

	const (
		producerCount   = 10
		jobsPerProducer = 1000
		consumerCount   = 10
	)

	totalJobs := producerCount * jobsPerProducer

	var producerWG sync.WaitGroup
	producerWG.Add(producerCount)

	for producer := 0; producer < producerCount; producer++ {
		go func(producerID int) {
			defer producerWG.Done()

			for i := 0; i < jobsPerProducer; i++ {
				job := Job{
					ID: producerID*jobsPerProducer + i,
				}

				if err := queue.Enqueue(job); err != nil {
					t.Errorf("unexpected enqueue error: %v", err)
				}
			}
		}(producer)
	}

	go func() {
		producerWG.Wait()
		queue.Close()
	}()

	var consumerWG sync.WaitGroup
	consumerWG.Add(consumerCount)

	results := make(map[int]bool)
	var resultsMu sync.Mutex

	for i := 0; i < consumerCount; i++ {
		go func() {
			defer consumerWG.Done()

			for {
				job, ok := queue.Dequeue()
				if !ok {
					return
				}

				resultsMu.Lock()
				if results[job.ID] {
					t.Errorf("job %d was processed more than once", job.ID)
				}
				results[job.ID] = true
				resultsMu.Unlock()
			}
		}()
	}

	consumerWG.Wait()

	if len(results) != totalJobs {
		t.Fatalf("expected %d jobs, got %d", totalJobs, len(results))
	}

	if got := queue.Len(); got != 0 {
		t.Fatalf("expected queue length 0, got %d", got)
	}
}
