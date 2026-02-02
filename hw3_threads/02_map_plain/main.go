package main

import (
	"fmt"
	"sync"
)

func main() {
	m := make(map[int]int)
	var wg sync.WaitGroup

	const goroutines = 50
	const perG = 1000

	wg.Add(goroutines)
	for g := range goroutines {
		go func(g int) {
			defer wg.Done()
			for i := range perG {
				m[g*perG+i] = i
			}
		}(g)
	}

	wg.Wait()
	fmt.Println("len(m):", len(m))
}