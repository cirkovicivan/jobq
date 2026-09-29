# JobQ
JobQ is a concurrent in-memory job queue written in Go that provides FIFO scheduling, thread-safe access, and configurable worker-based job processing.

[IMAGE]

## Motivation
I wanted to understand how job queues work under the hood, so I studied existing queue libraries. Since they handled the difficult parts for me, I built JobQ from scratch in Go to understand the design decisions behind these systems and what it takes to build one from the ground up.

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