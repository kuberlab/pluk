package io

import (
	"bytes"
	"crypto/sha512"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/utils"
)

type ChunkedReader struct {
	ChunkSize int
	reader    io.Reader
}

func NewChunkedReader(chunkSize int, reader io.Reader) *ChunkedReader {
	return &ChunkedReader{
		ChunkSize: chunkSize,
		reader:    reader,
	}
}

func (c *ChunkedReader) NextChunk() ([]byte, string, error) {
	data := make([]byte, c.ChunkSize)

	n, err := c.reader.Read(data)
	if n > 0 {
		res := data[:n]
		sum := sha512.Sum512(res)
		return res, fmt.Sprintf("%x", sum[:]), nil
	}
	if err != nil {
		return nil, "", err
	}
	return nil, "", io.EOF
}

func CheckChunk(hash string) bool {
	filePath := utils.GetHashedFilename(hash)
	_, err := os.Stat(filePath)
	return err == nil
}

func GetChunk(hash string) (io.ReadCloser, error) {
	filePath := utils.GetHashedFilename(hash)
	return os.Open(filePath)
}

func SaveChunk(hash string, data io.ReadCloser) error {
	filePath := utils.GetHashedFilename(hash)

	splitted := strings.Split(filePath, "/")
	baseDir := splitted[:len(splitted)-1]

	if err := os.MkdirAll(strings.Join(baseDir, "/"), os.ModePerm); err != nil {
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	logrus.Debugf("Created %v", filePath)

	defer file.Close()

	buf := bytes.NewBuffer([]byte{})
	var written int64
	var writer io.Writer = file
	if utils.HasMasters() {
		// If we have masters, then also write to buf in order to use it for further push.
		writer = io.MultiWriter(writer, buf)
	}
	written, err = io.Copy(writer, data)
	if err != nil {
		return err
	}
	data.Close()

	logrus.Debugf("Written %v bytes.", written)

	if utils.HasMasters() {
		// TODO: decide whether it can go in async
		MasterClient.SaveChunk(hash, buf.Bytes())
	}
	return nil
}
