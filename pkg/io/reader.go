package io

import (
	"io"
	"io/ioutil"
)

type ReaderInterface interface {
	io.ReadCloser
	io.Seeker
}

type ChunkReader struct {
	data   []byte
	offset int64
}

func NewChunkReaderFromCloser(closer io.ReadCloser) (ReaderInterface, error) {
	data, err := ioutil.ReadAll(closer)
	if err != nil {
		return nil, err
	}
	closer.Close()
	return &ChunkReader{data: data}, nil
}

func NewChunkReaderFromData(data []byte) ReaderInterface {
	return &ChunkReader{data: data}
}

func (r *ChunkReader) Read(p []byte) (n int, err error) {
	end := r.offset + int64(len(p))
	if end > int64(len(r.data)) {
		end = int64(len(r.data))
	}
	copy(p, r.data[r.offset:end])
	return int(end - r.offset), nil
}

func (r *ChunkReader) Close() error {
	return nil
}

func (r *ChunkReader) Seek(offset int64, whence int) (int64, error) {
	r.offset = offset
	return r.offset, nil
}
