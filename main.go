package main

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"

	"github.com/ettavolt/countipgo/lib"
)

// This is the dispatcher. It invokes splitter-converter,
// then takes its output and performs a series of merges.
// There are two constants in util.go that could be tuned for performance.
func main() {
	path := os.Args[1]
	splitFilesChan := make(chan string)
	sortedFilesChan := make(chan string)

	go lib.ConvertAndSplit(path, splitFilesChan)

	queueSize := atomic.Int32{}
	// There could be a way to achieve synchronization without the empty file at the end,
	// but it will be much more complex.
	// Supposedly, for a substantial processing task, empty file will be merged with a smaller split-off,
	// rather than a late-generation merge.
	queueSize.Store(1)
	go func() {
		for fileName := range splitFilesChan {
			queueSize.Add(1)
			sortedFilesChan <- fileName
		}
		// Send empty file to decrease the initial 1 in the queue.
		lib.WriteBytes(func() (uint32, bool) {
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
			go lib.MergeFiles(leftFile, file, sortedFilesChan)
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
