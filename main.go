package main

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"
)

func main() {
	path := os.Args[1]
	splitFilesChan := make(chan string)
	sortedFilesChan := make(chan string)

	go readAndConvert(path, splitFilesChan)

	queueSize := atomic.Int32{}
	queueSize.Store(1)
	go func() {
		for fileName := range splitFilesChan {
			queueSize.Add(1)
			sortedFilesChan <- fileName
		}
		// Send empty file to decrease the initial 1 in the queue.
		writeBytes(func() (uint32, bool) {
			return 0, true
		}, sortedFilesChan)
	}()

	leftFile := ""
	for queueSize.Load() > 0 {
		file := <-sortedFilesChan
		if leftFile == "" {
			queueSize.Add(-1)
			leftFile = file
		} else {
			// Don't change the queue size, we're adding one more file
			go mergeFiles(leftFile, file, sortedFilesChan)
			leftFile = ""
		}
	}
	close(sortedFilesChan)

	defer os.Remove(leftFile)
	stat, err := os.Stat(leftFile)
	if err != nil {
		log.Panic(err)
	}
	finalSize := stat.Size() / 4
	fmt.Printf("Size of the final sorted set (in terms of number of unique IP addresses): %d\n", finalSize)
}
