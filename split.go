package main

import (
	"bufio"
	"encoding/binary"
	"log"
	"net"
	"os"
	"sort"
	"sync"
)

func readAndConvert(filePath string, splitFilesChan chan<- string) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Panicf("Failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	wg := sync.WaitGroup{}
	var chunk []uint32

	for scanner.Scan() {
		ip := net.ParseIP(scanner.Text())
		if ip == nil {
			continue
		}
		ip = ip.To4() // Ensures it is an IPv4
		num := binary.BigEndian.Uint32(ip)
		chunk = append(chunk, num)
		if len(chunk) >= chunkSize {
			wg.Add(1)
			go sortAndWrite(chunk, &wg, splitFilesChan)
			chunk = make([]uint32, 0, chunkSize) // Reset chunk
		}
	}

	// Send any remaining IP addresses
	if len(chunk) > 0 {
		wg.Add(1)
		go sortAndWrite(chunk, &wg, splitFilesChan)
	}

	wg.Wait()
	close(splitFilesChan)
}

func sortAndWrite(chunk []uint32, wg *sync.WaitGroup, sortedFilesChan chan<- string) {
	// Remove duplicates and sort
	sort.Slice(chunk, func(i, j int) bool { return chunk[i] < chunk[j] })
	j := 0
	i := -1
	writeBytes(func() (uint32, bool) {
		for {
			i++
			if i >= len(chunk) {
				return 0, true
			}
			// The first part is only true at first invocation.
			if j != i && chunk[j] == chunk[i] {
				continue
			}
			j = i
			return chunk[i], false
		}
	}, sortedFilesChan)
	wg.Done()
}
