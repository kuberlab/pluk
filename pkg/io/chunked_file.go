package io

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"golang.org/x/net/webdav"
)

type ChunkedFile struct {
	f                  *os.File
	Root               string
	Dir                string
	size               int64
	Chunks             []chunk
	currentChunk       int
	currentChunkReader io.ReadCloser
	offset             int64 // current chunk offset
}

type chunk struct {
	path string
	size int64
}

func NewChunkedFile(f *os.File) (webdav.File, error) {
	file := &ChunkedFile{f: f}

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) < 2 {
		return f, nil
		//return nil, fmt.Errorf("Probably corrupted file [name=%v], contained less than 2 lines: %v", f.Name(), string(data))
	}

	size, err := strconv.ParseInt(lines[0], 10, 64)
	if err != nil {
		return f, nil
		//return nil, err
	}

	file.size = size
	file.Chunks = make([]chunk, 0)
	for _, chunkPath := range lines[1:] {
		info, err := os.Stat(chunkPath)
		if err != nil {
			return nil, err
		}
		file.Chunks = append(file.Chunks, chunk{chunkPath, info.Size()})
	}
	file.Dir = path.Dir(f.Name())
	return file, nil
}

func (f *ChunkedFile) Close() error {
	if f.currentChunkReader != nil {
		return f.currentChunkReader.Close()
	}
	return nil
}

func (f *ChunkedFile) Read(p []byte) (n int, err error) {
	var read int
	if f.currentChunkReader == nil {
		reader, err := os.Open(f.Chunks[f.currentChunk].path)
		if err != nil {
			return read, err
		}
		f.currentChunkReader = reader
	}

	var reader *os.File
	var r int
	for {
		r, err = f.currentChunkReader.Read(p[read:])
		read += r
		if err == io.EOF && f.currentChunk < (len(f.Chunks)-1) && read < len(p) {
			// Read more; current chunk is over.
			f.currentChunkReader.Close()
			f.currentChunk++
			reader, err = os.Open(f.Chunks[f.currentChunk].path)
			if err != nil {
				return read, err
			}
			f.currentChunkReader = reader
			err = nil
		} else {
			// either nothing to read or
			// all chunks are over or
			// buffer is full
			break
		}
	}

	return read, err
}

func (f *ChunkedFile) Seek(offset int64, whence int) (int64, error) {
	if (whence == io.SeekStart && offset > f.size) || (whence == io.SeekEnd && offset > 0) {
		return 0, fmt.Errorf("offset %v more than size of the file", offset)
	}

	if whence == io.SeekStart && offset < 0 {
		return 0, fmt.Errorf("seek before the start of the file")
	}

	if f.currentChunkReader != nil {
		f.currentChunkReader.Close()
		f.currentChunkReader = nil
	}

	switch whence {
	case io.SeekStart:
		ofs := offset
		for i, ch := range f.Chunks {
			if ofs-ch.size < 0 {
				f.currentChunk = i
				break
			}
			ofs -= ch.size
		}
		f.offset = ofs
	case io.SeekCurrent:
		ofs := f.offset + offset
		if offset < 0 {
			if f.currentChunk == 0 {
				return 0, fmt.Errorf("seek before the start of the file")
			}
			f.currentChunk--
			f.offset = f.Chunks[f.currentChunk].size - ofs
		} else if offset > f.Chunks[f.currentChunk].size {
			f.offset -= f.Chunks[f.currentChunk].size
			f.currentChunk++
		}
	case io.SeekEnd:
		ofs := -offset
		for i := len(f.Chunks) - 1; i >= 0; i-- {
			if ofs-f.Chunks[i].size < 0 {
				f.currentChunk = i
				break
			}
			ofs -= f.Chunks[i].size
		}
		f.offset = f.Chunks[f.currentChunk].size - ofs
	}

	return offset, nil
}

func (f *ChunkedFile) Readdir(count int) ([]os.FileInfo, error) {
	baseInfos, err := f.f.Readdir(count)
	if err != nil {
		return nil, err
	}
	infos := make([]os.FileInfo, 0)
	for _, baseInfo := range baseInfos {
		virtualFile, err := os.Open(baseInfo.Name())
		if err != nil {
			return nil, err
		}
		chunked, err := NewChunkedFile(virtualFile)
		if err != nil {
			return nil, err
		}
		info, err := chunked.Stat()
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func (f *ChunkedFile) Stat() (os.FileInfo, error) {
	baseStat, err := f.f.Stat()
	if err != nil {
		return nil, err
	}
	info := &ChunkedFileInfo{
		mode:    baseStat.Mode(),
		modTime: baseStat.ModTime(),
		name:    baseStat.Name(),
		dir:     baseStat.IsDir(),
	}
	info.size = f.size

	return info, nil
}

func (*ChunkedFile) Write(p []byte) (int, error) {
	return 0, errors.New("Read only")
}

// A ChunkedFileInfo is the implementation of FileInfo returned by Stat and Lstat.
type ChunkedFileInfo struct {
	dir     bool
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
}

func (fs *ChunkedFileInfo) Name() string       { return fs.name }
func (fs *ChunkedFileInfo) IsDir() bool        { return fs.dir }
func (fs *ChunkedFileInfo) Size() int64        { return fs.size }
func (fs *ChunkedFileInfo) Mode() os.FileMode  { return fs.mode }
func (fs *ChunkedFileInfo) ModTime() time.Time { return fs.modTime }
func (fs *ChunkedFileInfo) Sys() interface{}   { return nil }

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
