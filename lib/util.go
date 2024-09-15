package lib

import (
	"bufio"
	"encoding/binary"
	"errors"
	"log"
	"os"
)

const ChunkSize = 0x1000000
const IoBufferSize = 0x100000

// WriteBytes expects producer to either output next number or indicate the end with true as the second return value.
func WriteBytes(producer func() (uint32, bool), sortedFilesChan chan<- string) {
	tmpfile, err := os.CreateTemp("", "sorted")
	if err != nil {
		log.Panic(err)
	}
	writer := bufio.NewWriterSize(tmpfile, IoBufferSize)

	var buf [4]byte
	for {
		num, wasLast := producer()
		if wasLast {
			break
		}
		binary.BigEndian.PutUint32(buf[:], num)
		if _, err := writer.Write(buf[:]); err != nil {
			errClean := tmpfile.Close()
			log.Panic(errors.Join(err, errClean))
		}
	}

	if err := writer.Flush(); err != nil {
		log.Panic(err)
	}

	if errClean := tmpfile.Close(); errClean != nil {
		// Something has already closed the file, but everything is in there - no problem.
		log.Println(errClean)
	}
	// Now that the file is ready to be read, pass it on.
	sortedFilesChan <- tmpfile.Name()
}
