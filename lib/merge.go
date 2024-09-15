package lib

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"os"
)

func MergeFiles(leftFileName string, rightFileName string, sortedFilesChan chan<- string) {
	leftChan := make(chan uint32, 10)
	rightChan := make(chan uint32, 10)
	go consumeUint32s(leftFileName, leftChan)
	go consumeUint32s(rightFileName, rightChan)

	left, leftAvailable := <-leftChan
	right, rightAvailable := <-rightChan
	var lastValue uint32
	sentOne := false
	WriteBytes(func() (uint32, bool) {
		var ret uint32
		for {
			if leftAvailable {
				if rightAvailable && right < left {
					ret = right
					right, rightAvailable = <-rightChan
				} else {
					ret = left
					left, leftAvailable = <-leftChan
				}
			} else if rightAvailable {
				ret = right
				right, rightAvailable = <-rightChan
			} else {
				return 0, true
			}
			if sentOne && ret == lastValue {
				continue
			}
			sentOne = true
			lastValue = ret
			return ret, false
		}
	}, sortedFilesChan)
}

func consumeUint32s(fromFileName string, to chan<- uint32) {
	defer os.Remove(fromFileName)
	from, err := os.Open(fromFileName)
	if err != nil {
		log.Panicf("Failed to open from: %v", err)
	}
	defer from.Close()

	reader := bufio.NewReaderSize(from, IoBufferSize)

	defer close(to)

	var buf [4]byte
	for {
		nRead, err := reader.Read(buf[:])
		if err != nil && err != io.EOF {
			log.Panic(err)
		}
		if nRead != 4 {
			return
		}
		to <- binary.BigEndian.Uint32(buf[:])
	}
}
