package io

import (
	"bufio"
	"io/ioutil"
	"os"

	"github.com/Sirupsen/logrus"
)

func GetRealFileContent(path string) ([]byte, error) {
	data := make([]byte, 0)

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(f)
	for {
		line, _, _ := reader.ReadLine()
		if line == nil {
			break
		}
		// line = absolute path to chunk.
		// get chunk
		logrus.Debugf("Reading chunk at %v", string(line))
		chunkData, err := ioutil.ReadFile(string(line))
		if err != nil {
			return nil, err
		}
		data = append(data, chunkData...)
	}
	return data, nil
}
