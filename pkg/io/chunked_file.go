package io

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

type PlukClient interface {
	CheckChunk(hash string) (*types.ChunkCheck, error)
	CheckChunkWebsocket(hash string) (res *types.ChunkCheck, err error)
	CheckDataset(workspace, dataset string) (*types.Dataset, error)
	CheckWorkspace(workspace string) (*types.Workspace, error)
	Close() error
	DeleteDataset(workspace, name string) error
	DeleteVersion(workspace, name, version string) error
	DownloadChunk(hash string) (io.ReadCloser, error)
	DownloadDataset(workspace, name, version string, w io.Writer) error
	GetFSStructure(workspace, name, version string) (*ChunkedFileFS, error)
	ListDatasets(workspace string) (*types.DataSetList, error)
	ListVersions(workspace, datasetName string) (*types.VersionList, error)
	PrepareWebsocket() error
	SaveChunk(hash string, data []byte) error
	SaveChunkWebsocket(hash string, data []byte) error
	SaveFileStructure(structure types.FileStructure, workspace, name, version string, create bool) error
	WebdavAuth(user, pass, path string) (bool, error)
}

var MasterClient PlukClient

type ChunkedFileFS struct {
	lock  sync.RWMutex
	Root  string                    `json:"root"`
	Dirs  map[string]*ChunkedFileFS `json:"dirs"`  // Only dirs for current root
	Files map[string]*ChunkedFile   `json:"files"` // Only files for current root
}

func (fs *ChunkedFileFS) GetFile(absname string) *ChunkedFile {
	absname = strings.TrimPrefix(absname, "/")
	dirname := filepath.Dir(absname)
	filename := filepath.Base(absname)

	if absname == "" {
		return fs.dirObj("/" + absname)
	}
	curDir := fs.GetDir(dirname)
	if curDir == nil {
		return nil
	}
	if f, ok := curDir.Files[filename]; ok {
		return f
	} else {
		if _, ok := curDir.Dirs[filename]; ok {
			// Return file-dir object
			return fs.dirObj(absname)
		} else {
			return nil
		}
	}

	return nil
}

func (fs *ChunkedFileFS) dirObj(absname string) *ChunkedFile {
	return &ChunkedFile{
		Fstat: &ChunkedFileInfo{
			Fsize:    4096,
			Dir:      true,
			Fname:    filepath.Base(absname),
			Fmode:    0775,
			FmodTime: time.Now().Add(-time.Hour),
		},
		Size: 4096,
		Name: absname,
	}
}

func (fs *ChunkedFileFS) GetDir(dirname string) *ChunkedFileFS {
	if dirname == fs.Root || dirname == "." {
		return fs
	}

	dirname = strings.TrimPrefix(dirname, "/")
	splitted := strings.Split(dirname, "/")
	curDir := fs
	if dirname == "" {
		return curDir
	}
	for _, dir := range splitted {
		newDir, ok := curDir.Dirs[dir]
		if !ok {
			return nil
		}
		curDir = newDir
	}
	return curDir
}

func (fs *ChunkedFileFS) Walk(root string, walkFunc func(path string, f *ChunkedFile, err error) error) error {
	rootDir := fs.GetDir(root)
	if err := walkFunc(root, fs.dirObj(root), nil); err != nil {
		return err
	}
	for _, d := range rootDir.Dirs {
		if err := d.Walk(d.Root, walkFunc); err != nil {
			return err
		}
	}
	for _, f := range fs.Files {
		if err := walkFunc(filepath.Join(rootDir.Root, f.Name), f, nil); err != nil {
			return err
		}
	}
	return nil
}

func (fs *ChunkedFileFS) Prepare() {
	// For reflective calls
	//for _, f := range fs.FS {
	//	f.fs = fs
	//}
}

func (fs *ChunkedFileFS) AddDir(path string) {
	base := filepath.Base(path)
	_, ok := fs.Dirs[base]
	if !ok {
		fs.Dirs[base] = &ChunkedFileFS{
			Root:  path,
			Dirs:  make(map[string]*ChunkedFileFS),
			Files: make(map[string]*ChunkedFile),
		}
	}
}

func (fs *ChunkedFileFS) Clone() *ChunkedFileFS {
	cloned := &ChunkedFileFS{
		Files: make(map[string]*ChunkedFile),
		Dirs:  make(map[string]*ChunkedFileFS),
		Root:  fs.Root,
	}
	for k, f := range fs.Files {
		cloned.Files[k] = &ChunkedFile{
			Name:               f.Name,
			currentChunkReader: nil,
			currentChunk:       0,
			Chunks:             f.Chunks,
			Fstat:              f.Fstat,
			fs:                 cloned,
			offset:             0,
			Ref:                f.Ref,
			Size:               f.Size,
		}
	}
	for k, d := range fs.Dirs {
		cloned.Dirs[k] = d.Clone()
	}
	return cloned
}

func (fs *ChunkedFileFS) Readdir(prefix string, count int) ([]os.FileInfo, error) {
	prefix = strings.TrimPrefix(prefix, "/")

	dir := fs.GetDir(prefix)
	res := make([]os.FileInfo, 0)

	// Add all files and dirs within current directory
	for _, d := range dir.Dirs {
		res = append(res, dir.dirObj(d.Root).Fstat)
	}
	for _, f := range dir.Files {
		res = append(res, f.Fstat)
	}

	if count == 0 {
		count = len(res)
	}
	return res[:count], nil
}

type ChunkedFile struct {
	Ref                string  `json:"ref"`
	Name               string  `json:"name"`
	Size               int64   `json:"size"`
	Chunks             []Chunk `json:"chunks"`
	currentChunk       int
	currentChunkReader io.ReadCloser
	offset             int64 // absolute offset
	chunkOffset        int64

	Fstat *ChunkedFileInfo `json:"stat"`
	fs    *ChunkedFileFS
	lock  sync.RWMutex
}

type Chunk struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
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
			hash := utils.GetHashFromPath(chunkPath)
			//logrus.Debugf("download")
			//t := time.Now()
			reader, err = MasterClient.DownloadChunk(hash)

			if err != nil {
				return nil, err
			}
			if utils.SaveChunks() {
				data, err := ioutil.ReadAll(reader)
				if err != nil {
					return nil, err
				}
				//logrus.Debugf("download complete! %v", time.Since(t))
				reader.Close()
				SaveChunk(hash, ioutil.NopCloser(bytes.NewBuffer(data)), false)
				return ioutil.NopCloser(bytes.NewBuffer(data)), nil
			} else {
				return reader, nil
			}
		} else {
			return nil, err
		}
	}
	return reader, err
}

func (f *ChunkedFile) Read(p []byte) (n int, err error) {
	f.lock.Lock()
	defer f.lock.Unlock()
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
	// Shift read position to current offset
	f.currentChunkReader.Read(make([]byte, f.chunkOffset))
	f.chunkOffset = 0
	for {
		r, err = f.currentChunkReader.Read(p[read:])
		read += r
		if err == io.EOF && f.currentChunk < (len(f.Chunks)-1) && read < len(p) {
			// Read more; current chunk is over.
			f.currentChunkReader.Close()
			f.currentChunk++
			reader, err = f.getChunkReader(f.Chunks[f.currentChunk].Path)
			if err != nil {
				logrus.Error(err)
				f.currentChunkReader = nil
				return read, io.EOF
			}
			f.currentChunkReader = reader
			err = nil
		} else {
			// either nothing to read or
			// all chunks are over or
			// buffer is full
			if err == io.EOF && f.currentChunk >= len(f.Chunks)-1 {
				f.currentChunkReader.Close()
				f.currentChunk = 0
				f.currentChunkReader = nil
				return 0, io.EOF
			}
			break
		}
	}

	return read, err
}

func (f *ChunkedFile) Seek(offset int64, whence int) (res int64, err error) {
	f.lock.Lock()
	defer f.lock.Unlock()
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
			f.chunkOffset = ofs
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
