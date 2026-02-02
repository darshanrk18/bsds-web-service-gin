package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

func main() {
	const goroutines = 50
	const incPerG = 10000

	var normal int64
	var atom int64

	var wg sync.WaitGroup

	// Normal counter (race)
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range incPerG {
				normal++ // NOT atomic (race)
			}
		}()
	}

	// Atomic counter (safe)
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range incPerG {
				atomic.AddInt64(&atom, 1)
			}
		}()
	}

	wg.Wait()

	expected := int64(goroutines * incPerG)
	fmt.Println("Expected:", expected)
	fmt.Println("Normal  :", normal)
	fmt.Println("Atomic  :", atom)
}
