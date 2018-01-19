package io

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"bytes"
	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pacak/pkg/pacakimpl"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
	"golang.org/x/net/webdav"
	"golang.org/x/sync/semaphore"
)

type PlukClient interface {
	ListDatasets(workspace string) (*types.DataSetList, error)
	ListVersions(workspace, datasetName string) (*types.VersionList, error)
	CheckChunk(hash string) (*types.CheckChunkResponse, error)
	DownloadChunk(hash string) (io.ReadCloser, error)
	SaveChunk(hash string, data []byte) error
	GetFSStructure(workspace, name, version string) (*ChunkedFileFS, error)
	SaveFileStructure(structure types.FileStructure, workspace, name, version string) error
	DownloadDataset(workspace, name, version string, w io.Writer) error
	DeleteDataset(workspace, name string) error
	DeleteVersion(workspace, name, version string) error
	WebdavAuth(user, pass, path string) (bool, error)
}

var MasterClient PlukClient

type ChunkedFileFS struct {
	lock *sync.RWMutex
	FS   map[string]*ChunkedFile `json:"fs"`
}

type Dir struct {
}

type File struct {
}

func InitChunkedFSFromRepo(repo pacakimpl.PacakRepo, version string, gitFiles []os.FileInfo) (*ChunkedFileFS, error) {
	fs := &ChunkedFileFS{FS: make(map[string]*ChunkedFile), lock: &sync.RWMutex{}}

	var n int64 = utils.ReadConcurrency()
	sem := semaphore.NewWeighted(n)
	ctx := context.TODO()

	//errChan := make(chan error, 1000)
	addFile := func(gitFile os.FileInfo) {
		chunked, err := NewInternalChunked(repo, version, gitFile.Name())
		if err != nil {
			//errChan <- err
			logrus.Errorf("Read %v: %v", gitFile.Name(), err)
			return
		}
		chunked.Fstat = &ChunkedFileInfo{
			FmodTime: gitFile.ModTime(),
			Fmode:    gitFile.Mode(),
			Fsize:    chunked.Size,
			Fname:    path.Base(chunked.Name),
			Dir:      gitFile.IsDir(),
		}
		if gitFile.IsDir() {
			chunked.Fstat.Fsize = 4096
		}
		fs.lock.Lock()
		fs.FS[gitFile.Name()] = chunked
		fs.lock.Unlock()
		sem.Release(1)
	}

	for _, gitFile := range gitFiles {
		sem.Acquire(ctx, 1)
		go addFile(gitFile)
	}
	sem.Acquire(ctx, n)
	return fs, nil
}

func (fs *ChunkedFileFS) Prepare() {
	fs.AddRoot()
	for _, f := range fs.FS {
		f.fs = fs
	}
}

func (fs *ChunkedFileFS) AddRoot() {
	fs.FS["/"] = &ChunkedFile{
		Fstat: &ChunkedFileInfo{
			Fsize:    4096,
			Dir:      true,
			Fname:    "/",
			Fmode:    os.ModePerm,
			FmodTime: time.Now().Add(-time.Hour),
		},
		Size: 4096,
		Name: "/",
	}
}

func (fs *ChunkedFileFS) Readdir(prefix string, count int) ([]os.FileInfo, error) {
	// filter infos by prefix.
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	//logrus.Debugf("Search by prefix %v", prefix)
	res := make([]os.FileInfo, 0)
	for _, f := range fs.FS {
		if strings.HasPrefix(f.Name, prefix) && f.Name != prefix {
			path := strings.TrimPrefix(f.Name, prefix)
			// If there is a slash in 1+ position: exclude subdirs
			//logrus.Debugf("path = %v, index = %v", path, strings.Index(strings.TrimPrefix(path, "/"), "/"))
			if strings.Index(strings.TrimPrefix(path, "/"), "/") > 0 {
				continue
			}
			//logrus.Debugf("Include %v [name=%v]", f.Name, f.Fstat.Name())

			res = append(res, f.Fstat)
		}
	}

	if count == 0 {
		count = len(res)
	}
	return res[:count], nil
}

type ChunkedFile struct {
	//repo               pacakimpl.PacakRepo
	Ref                string  `json:"ref"`
	Name               string  `json:"name"`
	Size               int64   `json:"size"`
	Chunks             []chunk `json:"chunks"`
	currentChunk       int
	currentChunkReader io.ReadCloser
	offset             int64 // absolute offset

	Fstat *ChunkedFileInfo `json:"stat"`
	fs    *ChunkedFileFS
}

type chunk struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

func NewInternalChunked(repo pacakimpl.PacakRepo, ref, path string) (*ChunkedFile, error) {
	chunked, err := NewChunkedFileFromRepo(repo, ref, path)
	if err != nil {
		return nil, err
	}
	return chunked.(*ChunkedFile), nil
}

func NewChunkedFileFromRepo(repo pacakimpl.PacakRepo, ref, path string) (webdav.File, error) {
	file := &ChunkedFile{Name: path, Ref: ref}

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
		//return nil, fmt.Errorf("Probably corrupted file [Name=%v], contained less than 2 lines: %v", f.Name(), string(data))
	}

	size, err := strconv.ParseInt(lines[0], 10, 64)
	if err != nil {
		return file, nil
		//return nil, err
	}

	file.Size = size
	file.Chunks = make([]chunk, 0)
	for _, chunkPath := range lines[1:] {
		info, err := os.Stat(chunkPath)
		if err != nil {
			return nil, err
		}
		file.Chunks = append(file.Chunks, chunk{chunkPath, info.Size()})
	}
	//file.Dir = Path.Dir(f.Name())
	return file, nil
}

func (f *ChunkedFile) Close() error {
	if f.currentChunkReader != nil {
		return f.currentChunkReader.Close()
	}
	return nil
}

func (f *ChunkedFile) getChunkReader(chunkPath string) (reader io.ReadCloser, err error) {
	reader, err = os.Open(chunkPath)
	if err != nil {
		if os.IsNotExist(err) && utils.HasMasters() {
			// Read from master
			hash := strings.TrimPrefix(chunkPath, utils.DataDir())
			hash = strings.Replace(hash, "/", "", -1)
			//logrus.Debugf("download")
			//t := time.Now()
			reader, err = MasterClient.DownloadChunk(hash)

			if err != nil {
				return nil, err
			}
			data, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			//logrus.Debugf("download complete! %v", time.Since(t))
			reader.Close()
			go SaveChunk(hash, ioutil.NopCloser(bytes.NewBuffer(data)), false)
			return ioutil.NopCloser(bytes.NewBuffer(data)), nil
			//reader, err = os.Open(chunkPath)
			//fmt.Println("READER/ERR", reader, err)
			//return reader, err
		} else {
			return nil, err
		}
	}
	return reader, err
}

func (f *ChunkedFile) Read(p []byte) (n int, err error) {
	var read int
	var reader io.ReadCloser
	if f.currentChunkReader == nil {
		if len(f.Chunks) == 0 {
			return 0, io.EOF
		}
		reader, err = f.getChunkReader(f.Chunks[f.currentChunk].Path)
		if err != nil {
			logrus.Error(err)
			return read, io.EOF
		}
		f.currentChunkReader = reader
	}

	var r int
	for {
		r, err = f.currentChunkReader.Read(p[read:])
		read += r
		if err == io.EOF && f.currentChunk < (len(f.Chunks)-1) && read < len(p) {
			// Read more; current chunk is over.
			f.currentChunkReader.Close()
			f.currentChunk++
			reader, err = f.getChunkReader(f.Chunks[f.currentChunk].Path)
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
	if (whence == io.SeekStart && offset > f.Size) || (whence == io.SeekEnd && offset > 0) {
		return 0, fmt.Errorf("offset %v more than Size of the file", offset)
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
		absoluteOffset = f.Size - offset
	}

	ofs := absoluteOffset
	for i, ch := range f.Chunks {
		if ofs-ch.Size < 0 {
			f.currentChunk = i
			break
		}
		ofs -= ch.Size
	}
	f.offset = absoluteOffset

	return absoluteOffset, nil
}

func (f *ChunkedFile) Readdir(count int) ([]os.FileInfo, error) {
	return f.fs.Readdir(f.Name, count)
}

func (f *ChunkedFile) Stat() (os.FileInfo, error) {
	return f.Fstat, nil

	//t := time.Now()
	//baseStat, err := f.repo.StatFileAtRev(f.Ref, f.Name)
	//if err != nil {
	//	return nil, err
	//}
	//info := &ChunkedFileInfo{
	//	Fmode:    baseStat.Mode(),
	//	FmodTime: baseStat.ModTime(),
	//	Fname:    baseStat.Name(),
	//	Dir:      baseStat.IsDir(),
	//}
	//if baseStat.IsDir() {
	//	info.Fsize = 4096
	//} else {
	//	info.Fsize = f.Size
	//}
	//
	////fmt.Println("STAT", f.Name, time.Since(t), *info)
	//return info, nil
}

func (*ChunkedFile) Write(p []byte) (int, error) {
	return 0, errors.New("Read only")
}

// A ChunkedFileInfo is the implementation of FileInfo returned by Stat and Lstat.
type ChunkedFileInfo struct {
	Dir      bool        `json:"dir"`
	Fname    string      `json:"name"`
	Fsize    int64       `json:"size"`
	Fmode    os.FileMode `json:"mode"`
	FmodTime time.Time   `json:"modtime"`
}

func (fs *ChunkedFileInfo) Name() string       { return fs.Fname }
func (fs *ChunkedFileInfo) IsDir() bool        { return fs.Dir }
func (fs *ChunkedFileInfo) Size() int64        { return fs.Fsize }
func (fs *ChunkedFileInfo) Mode() os.FileMode  { return fs.Fmode }
func (fs *ChunkedFileInfo) ModTime() time.Time { return fs.FmodTime }
func (fs *ChunkedFileInfo) Sys() interface{}   { return nil }
