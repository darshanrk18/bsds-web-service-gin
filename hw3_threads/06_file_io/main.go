package main

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

func writeUnbuffered(filename string) time.Duration {
	f, _ := os.Create(filename)
	start := time.Now()

	for range 100000 {
		f.Write([]byte("hello world\n"))
	}

	f.Close()
	return time.Since(start)
}

func writeBuffered(filename string) time.Duration {
	f, _ := os.Create(filename)
	w := bufio.NewWriter(f)
	start := time.Now()

	for i := 0; i < 100000; i++ {
		w.WriteString("hello world\n")
	}

	w.Flush()
	f.Close()
	return time.Since(start)
}

func main() {
	t1 := writeUnbuffered("unbuffered.txt")
	t2 := writeBuffered("buffered.txt")

	fmt.Println("Unbuffered time:", t1)
	fmt.Println("Buffered time  :", t2)
}