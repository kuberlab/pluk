package io

import (
	"crypto/sha512"
	"fmt"
	"io"
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
