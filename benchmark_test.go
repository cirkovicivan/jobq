package jobq

import (
	"sync"
	"testing"
	"time"
)

func BenchmarkQueueThroughput(b *testing.B) {
	const (
		workerCount = 16
		jobCount    = 1_000_000
	)

	for b.Loop() {
		q := NewQueue()

		workers := make([]*Worker, workerCount)

		for i := range workerCount {
			w, err := NewWorker(q, func(j Job) error {
				return nil
			})

			if err != nil {
				b.Fatalf("unexpected error creating worker: %v", err)
			}

			workers[i] = w
		}

		var wg sync.WaitGroup
		wg.Add(workerCount)

		for _, w := range workers {
			go func() {
				defer wg.Done()
				w.Start()
			}()
		}

		start := time.Now()

		for i := range jobCount {
			if err := q.Enqueue(Job{
				ID:   i,
				Name: "benchmark",
			}); err != nil {
				b.Fatalf("unexpected enqueue error: %v", err)
			}
		}

		q.Close()
		wg.Wait()

		elapsed := time.Since(start)

		b.ReportMetric(
			float64(jobCount)/elapsed.Seconds(),
			"jobs/sec",
		)
	}
}
