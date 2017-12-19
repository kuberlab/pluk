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
	i, n := 0, 0
	var err error
	h := sha512.New()
	for err != io.EOF || i < c.ChunkSize {
		n, err = c.reader.Read(data[i:])
		if err != nil && err != io.EOF {
			return nil, "", err
		}
		if n > 0 {
			h.Write(data[i : i+n])
			i = i + n
		}
	}
	sum := h.Sum(nil)
	return data, fmt.Sprintf("%x", sum[:]), err
}
