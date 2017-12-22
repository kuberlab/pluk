package io

import (
	"bufio"
	"io/ioutil"
	"os"

	"fmt"
	"io"
	"strconv"

	"github.com/Sirupsen/logrus"
)

func GetRealFileReader(path string) (uint64, *bufio.Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, nil, err
	}
	reader := bufio.NewReader(f)
	line, _, _ := reader.ReadLine()
	if line == nil {
		return 0, nil, fmt.Errorf("File %v is probably empty.", path)
	}

	size, err := strconv.ParseUint(string(line), 10, 64)
	if err != nil {
		return 0, nil, err
	}
	return size, reader, nil
}

func GetNextRealChunk(reader *bufio.Reader) ([]byte, error) {
	line, _, _ := reader.ReadLine()
	if line == nil {
		return nil, io.EOF
	}
	// line = absolute path to chunk.
	// get chunk
	logrus.Debugf("Reading chunk at %v", string(line))
	chunkData, err := ioutil.ReadFile(string(line))
	if err != nil {
		return nil, err
	}
	return chunkData, nil
}
