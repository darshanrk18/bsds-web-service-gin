package main

import (
	"fmt"
	"runtime"
	"time"
)

func pingPong() time.Duration {
	ch := make(chan struct{})
	const rounds = 1_000_000

	start := time.Now()

	go func() {
		for range rounds {
			ch <- struct{}{}
			<-ch
		}
	}()

	for range rounds {
		<-ch
		ch <- struct{}{}
	}

	return time.Since(start)
}

func main() {
	fmt.Println("Running with 1 OS thread...")
	runtime.GOMAXPROCS(1)
	t1 := pingPong()
	fmt.Println("Time (1 thread):", t1)

	fmt.Println("\nRunning with all CPUs...")
	runtime.GOMAXPROCS(runtime.NumCPU())
	t2 := pingPong()
	fmt.Println("Time (many threads):", t2)
}