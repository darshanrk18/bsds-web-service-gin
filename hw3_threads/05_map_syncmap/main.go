package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	var m sync.Map
	var wg sync.WaitGroup

	const goroutines = 50
	const perG = 1000

	start := time.Now()

	wg.Add(goroutines)
	for g := range goroutines {
		go func(g int) {
			defer wg.Done()
			for i := range perG {
				m.Store(g*perG+i, i)
			}
		}(g)
	}

	wg.Wait()

	count := 0
	m.Range(func(_, _ any) bool {
		count++
		return true
	})

	elapsed := time.Since(start)

	fmt.Println("len(m):", count)
	fmt.Println("time  :", elapsed)
}