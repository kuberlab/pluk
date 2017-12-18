package types

import (
	"crypto/sha512"
	"fmt"
	"hash"
	"io"
	"os"
	"time"
)

type FS struct {
	Files []VirtualFile
}
type VirtualContent []string

type VirtualFile struct {
	Size     int64
	Name     string
	Content  VirtualContent
	Mode     os.FileMode
	ModeTime time.Time
}

type FileUploader interface {
	Upload(file string) (VirtualContent, error)
}

type ChunkedReader struct {
	Chunks    []string
	ChunkSize int
	reader    io.Reader
	i         int
	h         hash.Hash
}

func NewChunkedReader(chunkSize int, reader io.Reader) *ChunkedReader {
	return &ChunkedReader{
		Chunks:    []string{},
		ChunkSize: chunkSize,
		reader:    reader,
		h:         sha512.New(),
	}
}
func (c *ChunkedReader) Read(p []byte) (n int, err error) {
	n, err = c.reader.Read(p)
	if err != nil && err != io.EOF {
		return n,err
	}
	m := n
	i := 0
	for m > 0 {
		if (c.i + m) < c.ChunkSize {
			c.h.Write(p[i : i+m])
			c.i += m
			break

		} else {
			c.h.Write(p[i : i+c.ChunkSize-c.i])
			sum := c.h.Sum(nil)
			c.h.Reset()
			c.Chunks = append(c.Chunks, fmt.Sprintf("%x", sum[:]))
			i = i + c.ChunkSize - c.i
			m = m - c.ChunkSize + c.i
			c.i = 0
		}
	}
	if err == io.EOF && c.i > 0 {
		sum := c.h.Sum(nil)
		c.h.Reset()
		c.Chunks = append(c.Chunks, fmt.Sprintf("%x", sum[:]))
	}
	return
}

