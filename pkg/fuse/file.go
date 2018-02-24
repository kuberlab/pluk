package fuse

import (
	"io"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	plukio "github.com/kuberlab/pluk/pkg/io"
)

type PlukFile struct {
	nodefs.File
	chunked *plukio.ChunkedFile
}

func NewPlukFile(chunked *plukio.ChunkedFile) *PlukFile {
	return &PlukFile{
		File:    nodefs.NewDefaultFile(),
		chunked: chunked,
	}
}

func (f *PlukFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	return newResultData(f.chunked, dest, off), fuse.OK
}

func (f *PlukFile) Flush() fuse.Status {
	f.chunked.Close()
	return fuse.OK
}

func (f *PlukFile) GetAttr(a *fuse.Attr) fuse.Status {
	var mode uint32
	if f.chunked.Fstat.IsDir() {
		mode = fuse.S_IFDIR | uint32(f.chunked.Fstat.Mode())
	} else {
		mode = fuse.S_IFREG | uint32(f.chunked.Fstat.Mode())
	}
	a.Mode = mode
	a.Size = uint64(f.chunked.Size)
	a.Atime = uint64(f.chunked.Fstat.ModTime().Unix())
	a.Ctime = uint64(f.chunked.Fstat.ModTime().Unix())
	a.Mtime = uint64(f.chunked.Fstat.ModTime().Unix())
	return fuse.OK
}

type ResultData struct {
	data []byte
	size int
}

func newResultData(f *plukio.ChunkedFile, buf []byte, off int64) *ResultData {
	f.Seek(off, io.SeekStart)
	n, err := f.Read(buf)
	if err != nil {
		return &ResultData{buf, n}
	}
	return &ResultData{buf, n}
}

func (r *ResultData) Bytes(buf []byte) ([]byte, fuse.Status) {
	return r.data, fuse.OK
}

func (r *ResultData) Size() int {
	return r.size
}

func (r *ResultData) Done() {}
