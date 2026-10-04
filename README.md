# JobQ
JobQ is a concurrent in-memory job queue written in Go that provides FIFO scheduling, thread-safe access, and configurable worker-based job processing.

![JobQ Architecture](https://i.imgur.com/syl48kh.png)

## Motivation
I wanted to understand how job queues work under the hood, so I studied existing queue libraries. Since they handled the difficult parts for me, I built JobQ from scratch in Go to understand the design decisions behind these systems and what it takes to build one from the ground up.

## **⚙️ Engineering Highlights**

- **Thread-Safety:** Protected shared queue state with `sync.Mutex`.
- **Efficient Blocking:** Used `sync.Cond` so workers wait for jobs instead of continuously polling the queue.
- **Concurrent Processing:** Multiple workers can safely consume jobs from the same queue.
- **Race Detection:** Tested concurrent behavior with Go's race detector using `go test -race ./...`.
- **Graceful Shutdown:** Queue closing allows waiting workers to stop cleanly.

## 🚀 Quick Start

#### 1. Clone the repository

```bash
git clone https://github.com/cirkovicivan/jobq.git
cd jobq
```

#### 2. Run the tests

```bash
go test ./...
```

#### 3. Run the race detector

```bash
go test -race ./...
```

## 📖 Usage

### Job

#### Create a job
```go
job := Job{
    ID:   1,
    Name: "Send email",
}
```

### Queue

#### Create a queue

```go
q := NewQueue()
```

#### Add a job

```go
q.Enqueue(job) 
```

#### Retrieve a job

```go
job, ok := q.Dequeue()
```
### Worker

#### Create a worker

```go
w := NewWorker(q, func(job Job) error {
    // Process job
    return nil
})
```

#### Start a worker
```go
w.Start()
```


## 🤝 Contributing

If you'd like to contribute, please fork the repository and open a pull request to the `main` branch.
