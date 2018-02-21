package fuse

import (
	"math"
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

func (fs *PlukFS) String() string {
	return "plukefs"
}

func (fs *PlukFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	fs.lock.RLock()
	f := fs.innerFS.GetFile(name)
	fs.lock.RUnlock()
	if f == nil {
		return nil, fuse.ENOENT
	}
	var mode uint32
	if f.Fstat.IsDir() {
		mode = fuse.S_IFDIR | uint32(f.Fstat.Mode())
	} else {
		mode = fuse.S_IFREG | uint32(f.Fstat.Mode())
	}
	return &fuse.Attr{
		Size:    uint64(f.Size),
		Mode:    mode,
		Atime:   uint64(f.Fstat.ModTime().Unix()),
		Ctime:   uint64(f.Fstat.ModTime().Unix()),
		Mtime:   uint64(f.Fstat.ModTime().Unix()),
		Blocks:  uint64(math.Ceil(float64(f.Size) / 512.0)),
		Blksize: 1,
	}, fuse.OK
}

func (fs *PlukFS) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	if flags&fuse.O_ANYWRITE != 0 {
		return nil, fuse.EPERM
	}

	name = "/" + name
	fs.lock.RLock()
	f := fs.innerFS.GetFile(name)
	fs.lock.RUnlock()
	if f == nil {
		return nil, fuse.ENOENT
	}

	return NewPlukFile(f), fuse.OK
}

func (fs *PlukFS) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	name = "/" + name

	//t := time.Now()
	infos, err := fs.innerFS.Readdir(name, 0)
	//fmt.Print("OPENDIR ", time.Since(t))
	//fmt.Println()
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

func (fs *PlukFS) StatFs(name string) *fuse.StatfsOut {

	//  struct statvfs {
	//    unsigned long  f_bsize;    /* Filesystem block size */
	//    unsigned long  f_frsize;   /* Fragment size */
	//    fsblkcnt_t     f_blocks;   /* Size of fs in f_frsize units */
	//    fsblkcnt_t     f_bfree;    /* Number of free blocks */
	//    fsblkcnt_t     f_bavail;   /* Number of free blocks for
	//                                        unprivileged users */
	//    fsfilcnt_t     f_files;    /* Number of inodes */
	//    fsfilcnt_t     f_ffree;    /* Number of free inodes */
	//    fsfilcnt_t     f_favail;   /* Number of free inodes for
	//                                        unprivileged users */
	//    unsigned long  f_fsid;     /* Filesystem ID */
	//    unsigned long  f_flag;     /* Mount flags */
	//    unsigned long  f_namemax;  /* Maximum filename length */
	//  }
	var chunks uint64
	var files uint64
	var size uint64
	fs.innerFS.Walk("/"+name, func(path string, f *io.ChunkedFile, err error) error {
		if f.Fstat.IsDir() {
			return nil
		}
		files++
		chunks += uint64(len(f.Chunks))
		size += uint64(f.Size)
		return nil
	})

	return &fuse.StatfsOut{
		Files:   files,
		Bavail:  0,
		Bfree:   0,
		Blocks:  size,
		Bsize:   1,
		Ffree:   0,
		Frsize:  1,
		NameLen: 256,
	}
}
