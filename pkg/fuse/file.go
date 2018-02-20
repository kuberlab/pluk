package fuse

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/kuberlab/pluk/pkg/io"
)

type PlukFile struct {
	nodefs.File
	chunked *io.ChunkedFile
}

func NewPlukFile(chunked *io.ChunkedFile) *PlukFile {
	return &PlukFile{
		File:    nodefs.NewDefaultFile(),
		chunked: chunked,
	}
}

func (f *PlukFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	return nil, fuse.ENOSYS
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
