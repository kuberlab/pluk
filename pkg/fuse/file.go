package fuse

import (
	"io"

	"github.com/Sirupsen/logrus"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	plukio "github.com/kuberlab/pluk/pkg/io"
)

type PlukFile struct {
	nodefs.File
	chunked *plukio.ChunkedFile
	data    []byte
	size    int
}

var defFile = nodefs.NewDefaultFile()

func NewPlukFile(chunked *plukio.ChunkedFile) *PlukFile {
	return &PlukFile{
		File:    defFile,
		chunked: chunked,
	}
}

func (f *PlukFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	logrus.Debugf("READ %v, SIZE %v, OFFSET %v", f.chunked.Name, len(dest), off)
	return f.resultData(dest, off), fuse.OK
}

func (f *PlukFile) Flush() fuse.Status {
	f.chunked.Close()
	return fuse.OK
}

func (f *PlukFile) GetAttr(a *fuse.Attr) fuse.Status {
	var mode uint32
	if f.chunked.Dir {
		mode = fuse.S_IFDIR | f.chunked.Mode
	} else {
		mode = fuse.S_IFREG | f.chunked.Mode
	}
	a.Mode = mode
	a.Size = uint64(f.chunked.Size)
	a.Atime = uint64(f.chunked.ModTime.Unix())
	a.Ctime = uint64(f.chunked.ModTime.Unix())
	a.Mtime = uint64(f.chunked.ModTime.Unix())
	return fuse.OK
}

type ResultData struct {
	data []byte
	size int
}

func (f *PlukFile) resultData(buf []byte, off int64) fuse.ReadResult {
	//f.Seek(off, io.SeekStart)
	//n, err := f.Read(buf)
	// Squash seek & read into 1 operation since we can get
	// parallel request for reading 1 file:
	// Crashing sequence for parallel read 500 & 300:
	// SEEK 500
	// SEEK 0
	// READ 300
	// READ 500
	// Operations above must be in correct order:
	// SEEK 500
	// READ 300
	// SEEK 0
	// READ 500
	n, err := f.chunked.SeekAndRead(buf, off)
	f.size = n
	f.data = buf[:n]
	if err != nil {
		if err != io.EOF {
			logrus.Errorf("Read error: %v", err)
		}
		return f
	}
	return f
}

func (f *PlukFile) Bytes(buf []byte) ([]byte, fuse.Status) {
	return f.data, fuse.OK
}

func (f *PlukFile) Size() int {
	return f.size
}

func (f *PlukFile) Done() {}
