package io

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/sirupsen/logrus"
)

type ReaderInterface interface {
	io.ReadCloser
	io.Seeker
	DataSize() int64
}

type ChunkReader struct {
	data   []byte
	offset int64
}

type FileReader struct {
	*os.File
}

func (r *FileReader) DataSize() int64 {
	return -1
}

func NewChunkReaderFromCloser(closer io.ReadCloser) (ReaderInterface, error) {
	data, err := ioutil.ReadAll(closer)
	if err != nil {
		return nil, err
	}
	if err = closer.Close(); err != nil {
		logrus.Errorf("Can't close: %v", err)
	}
	return &ChunkReader{data: data}, nil
}

func NewChunkReaderFromData(data []byte) ReaderInterface {
	return &ChunkReader{data: data}
}

func NewReaderFromFile(f *os.File) ReaderInterface {
	return &FileReader{File: f}
}

func (r *ChunkReader) Read(p []byte) (n int, err error) {
	end := r.offset + int64(len(p))
	if end > int64(len(r.data)) {
		end = int64(len(r.data))
	}
	if end-r.offset <= 0 {
		return 0, io.EOF
	}
	copy(p, r.data[r.offset:end])

	copied := end - r.offset
	r.offset = end
	return int(copied), nil
}

func (r *ChunkReader) Close() error {
	return nil
}

func (r *ChunkReader) DataSize() int64 {
	return int64(len(r.data))
}

func (r *ChunkReader) Seek(offset int64, whence int) (int64, error) {
	r.offset = offset
	return r.offset, nil
}
