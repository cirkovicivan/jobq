package main

import (
	"fmt"

	"github.com/cirkovicivan/jobq"
)

func main() {
	q := jobq.NewQueue()

	fmt.Println("JobQ demo:", q)
}
