package fuse

import (
	"sync"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/plukclient"
)

type PlukFS struct {
	pathfs.FileSystem
	workspace string
	dataset   string
	version   string
	server    string
	secret    string
	client    io.PlukClient
	lock      sync.RWMutex
	innerFS   *io.ChunkedFileFS
}

func NewPlukFS(workspace, dataset, version, server, secret string) (pathfs.FileSystem, error) {
	fs := &PlukFS{
		FileSystem: pathfs.NewDefaultFileSystem(),
		workspace:  workspace,
		dataset:    dataset,
		version:    version,
		server:     server,
		secret:     secret,
	}

	client, err := plukclient.NewClient(server, &plukclient.AuthOpts{Workspace: workspace, Secret: secret})
	if err != nil {
		return nil, err
	}
	fs.client = client
	innerFS, err := client.GetFSStructure(workspace, dataset, version)
	if err != nil {
		return nil, err
	}
	innerFS.Prepare()
	fs.innerFS = innerFS

	return fs, nil
}

func (fs *PlukFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	name = "/" + name
	fs.lock.RLock()
	f, ok := fs.innerFS.FS[name]
	fs.lock.RUnlock()
	if !ok {
		return nil, fuse.ENOENT
	}
	var mode uint32
	if f.Fstat.IsDir() {
		mode = fuse.S_IFDIR | uint32(f.Fstat.Mode())
	} else {
		mode = fuse.S_IFREG | uint32(f.Fstat.Mode())
	}
	return &fuse.Attr{
		Size:  uint64(f.Size),
		Mode:  mode,
		Atime: uint64(f.Fstat.ModTime().Unix()),
		Ctime: uint64(f.Fstat.ModTime().Unix()),
		Mtime: uint64(f.Fstat.ModTime().Unix()),
	}, fuse.OK
}

func (fs *PlukFS) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	if flags&fuse.O_ANYWRITE != 0 {
		return nil, fuse.EPERM
	}

	name = "/" + name
	fs.lock.RLock()
	f, ok := fs.innerFS.FS[name]
	fs.lock.RUnlock()
	if !ok {
		return nil, fuse.ENOENT
	}

	return NewPlukFile(f), fuse.OK
}

func (fs *PlukFS) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	name = "/" + name

	//fs.lock.RLock()
	//f, ok := fs.innerFS.FS[name]
	//fs.lock.RUnlock()
	//if !ok {
	//	return nil, fuse.ENOENT
	//}
	//t := time.Now()
	infos, err := fs.innerFS.Readdir(name, 0)
	//fmt.Println("OPENDIR", time.Since(t))
	//infos, err := f.Readdir(0)
	if err != nil {
		return nil, fuse.ENODATA
	}
	res := make([]fuse.DirEntry, 0)
	for _, info := range infos {
		e := fuse.DirEntry{
			Mode: uint32(info.Mode()),
			Name: info.Name(),
		}
		res = append(res, e)
	}
	return res, fuse.OK
}
