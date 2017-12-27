package io

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kuberlab/pacak/pkg/pacakimpl"
	"golang.org/x/net/webdav"
)

type ChunkedFile struct {
	repo               pacakimpl.PacakRepo
	ref                string
	name               string
	size               int64
	Chunks             []chunk
	currentChunk       int
	currentChunkReader io.ReadCloser
	offset             int64 // absolute offset
}

type chunk struct {
	path string
	size int64
}

func NewChunkedFileFromRepo(repo pacakimpl.PacakRepo, ref, path string) (webdav.File, error) {
	file := &ChunkedFile{name: path, repo: repo, ref: ref}

	if path == "/" {
		return file, nil
	}
	data, err := repo.GetFileDataAtRev(ref, path)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(data), "\n")
	if len(lines) < 2 {
		return file, nil
		//return nil, fmt.Errorf("Probably corrupted file [name=%v], contained less than 2 lines: %v", f.Name(), string(data))
	}

	size, err := strconv.ParseInt(lines[0], 10, 64)
	if err != nil {
		return file, nil
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
	//file.Dir = path.Dir(f.Name())
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

func (f *ChunkedFile) Seek(offset int64, whence int) (res int64, err error) {
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

	var absoluteOffset int64
	switch whence {
	case io.SeekStart:
		absoluteOffset = offset
	case io.SeekCurrent:
		absoluteOffset = f.offset + offset
	case io.SeekEnd:
		absoluteOffset = f.size - offset
	}

	ofs := absoluteOffset
	for i, ch := range f.Chunks {
		if ofs-ch.size < 0 {
			f.currentChunk = i
			break
		}
		ofs -= ch.size
	}
	f.offset = absoluteOffset

	return absoluteOffset, nil
}

func (f *ChunkedFile) Readdir(count int) ([]os.FileInfo, error) {
	infos, err := f.repo.ListFilesAtRev(f.ref)
	if err != nil {
		return nil, err
	}

	// filter infos by path.
	path := strings.TrimPrefix(f.name, "/")

	res := make([]os.FileInfo, 0)
	for _, fi := range infos {
		if strings.HasPrefix(fi.Name(), path) && !strings.Contains(fi.Name(), "/") {
			res = append(res, fi)
		}
	}

	if count == 0 {
		count = len(res)
	}
	return res[:count], nil
}

func (f *ChunkedFile) Stat() (os.FileInfo, error) {
	baseStat, err := f.repo.StatFileAtRev(f.ref, f.name)
	if err != nil {
		return nil, err
	}
	info := &ChunkedFileInfo{
		mode:    baseStat.Mode(),
		modTime: baseStat.ModTime(),
		name:    baseStat.Name(),
		dir:     baseStat.IsDir(),
	}
	if baseStat.IsDir() {
		info.size = baseStat.Size()
	} else {
		info.size = f.size
	}

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
