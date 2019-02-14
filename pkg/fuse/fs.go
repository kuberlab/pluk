package fuse

import (
	"fmt"
	"math"
	"regexp"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/plukclient"
)

var ChangeDatasetRegex = regexp.MustCompile("\\.([-.a-z0-9_A-Z]+)__([-a-z0-9.]+)")

type PlukeFS struct {
	pathfs.FileSystem
	workspace       string
	dataset         string
	version         string
	server          string
	secret          string
	secretWorkspace string
	dsType          string
	client          io.PlukClient
	//lock            sync.RWMutex
	innerFS *io.ChunkedFileFS
}

func NewPlukFS(dsType, workspace, dataset, version, server, secret, secretWorkspace string) (pathfs.FileSystem, error) {
	if dsType == "" {
		dsType = "dataset"
	}

	fs := &PlukeFS{
		FileSystem:      pathfs.NewDefaultFileSystem(),
		workspace:       workspace,
		dataset:         dataset,
		version:         version,
		server:          server,
		secret:          secret,
		secretWorkspace: secretWorkspace,
		dsType:          dsType,
	}

	client, err := plukclient.NewClient(server, &plukclient.AuthOpts{Workspace: secretWorkspace, Secret: secret})
	if err != nil {
		return nil, err
	}
	fs.client = client
	innerFS, err := client.GetFSStructure(dsType, workspace, dataset, version)
	if err != nil {
		return nil, err
	}
	innerFS.Prepare()
	fs.innerFS = innerFS

	return fs, nil
}

func (fs *PlukeFS) String() string {
	return "plukefs"
}

func (fs *PlukeFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	//t := time.Now()
	//fmt.Println("GETATTR", name)

	//fs.lock.RLock()
	f := fs.innerFS.GetFile(name)
	//fs.lock.RUnlock()
	//fmt.Printf("GetFile: %v\n", time.Since(t))
	if f == nil {
		logrus.Errorf("File not found: %v", name)
		return nil, fuse.ENOENT
		//return fs.serviceGetAttr(name)
	}
	var mode int
	if f.Dir {
		mode = fuse.S_IFDIR | int(f.Mode)
	} else {
		mode = fuse.S_IFREG | int(f.Mode)
	}
	//fmt.Printf("GetAttr: %v\n", time.Since(t))
	unix := uint64(f.ModTime.Unix())
	return &fuse.Attr{
		Size:    uint64(f.Size),
		Mode:    uint32(mode),
		Atime:   unix,
		Ctime:   unix,
		Mtime:   unix,
		Blocks:  uint64(math.Ceil(float64(f.Size) / 512.0)),
		Blksize: 1,
	}, fuse.OK
}

func (fs *PlukeFS) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	if flags&fuse.O_ANYWRITE != 0 {
		return nil, fuse.EPERM
	}
	//fmt.Println("OPEN", name)
	//fullName := "/" + name
	//fs.lock.RLock()
	f := fs.innerFS.GetFile(name)
	//fs.lock.RUnlock()
	if f == nil {
		logrus.Errorf("File not found: %v", name)
		// Maybe service filename?
		return nil, fuse.ENOENT
		//return fs.serviceFileRead(name)
	}

	return NewPlukFile(f), fuse.OK
}

func (fs *PlukeFS) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	name = "/" + name

	//t := time.Now()
	files, err := fs.innerFS.ReaddirFiles(name, 0)
	//fmt.Println("OPENDIR", name, time.Since(t))
	if err != nil {
		return nil, fuse.ENODATA
	}
	res := make([]fuse.DirEntry, 0)
	for _, f := range files {
		e := fuse.DirEntry{
			Mode: uint32(f.Mode),
			Name: f.Name,
		}
		res = append(res, e)
	}
	return res, fuse.OK
}

func (fs *PlukeFS) StatFs(name string) *fuse.StatfsOut {

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
	_ = fs.innerFS.Walk("/"+name, func(path string, f *io.ChunkedFile, err error) error {
		if f.Dir {
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

func (fs *PlukeFS) serviceGetAttr(filename string) (*fuse.Attr, fuse.Status) {
	attr := &fuse.Attr{
		Mode:  fuse.S_IFREG | 0644,
		Atime: uint64(time.Now().Unix()),
		Ctime: uint64(time.Now().Unix()),
		Mtime: uint64(time.Now().Unix()),
	}

	if ChangeDatasetRegex.MatchString(filename) {
		attr.Size = 12
		return attr, fuse.OK
	}

	switch filename {
	case ".current_version":
		attr.Size = uint64(len(fs.version))
		return attr, fuse.OK
	case ".current_dataset":
		attr.Size = uint64(len(fs.dataset))
		return attr, fuse.OK
	case ".current_workspace":
		attr.Size = uint64(len(fs.workspace))
		return attr, fuse.OK
	}

	return nil, fuse.ENOENT
}

func (fs *PlukeFS) serviceFileRead(filename string) (file nodefs.File, code fuse.Status) {
	groups := ChangeDatasetRegex.FindStringSubmatch(filename)
	if len(groups) >= 3 {
		return fs.tryChangeDataset(filename)
	}

	switch filename {
	case ".current_version":
		return nodefs.NewDataFile([]byte(fs.version + "\n")), fuse.OK
	case ".current_dataset":
		return nodefs.NewDataFile([]byte(fs.dataset + "\n")), fuse.OK
	case ".current_workspace":
		return nodefs.NewDataFile([]byte(fs.workspace + "\n")), fuse.OK
	}

	return nil, fuse.ENOENT
}

func (fs *PlukeFS) tryChangeDataset(filename string) (file nodefs.File, code fuse.Status) {
	if !ChangeDatasetRegex.MatchString(filename) {
		return nil, fuse.ENOENT
	}
	// Change the dataset/version to the provided dataset/version:
	// Download new FS.
	groups := ChangeDatasetRegex.FindStringSubmatch(filename)
	if len(groups) < 3 {
		return nil, fuse.ENOENT
	}
	dataset := groups[1]
	version := groups[2]

	// Change dataset only if current dataset/version differs from target.
	if dataset != fs.dataset || version != fs.version {
		newFS, err := fs.client.GetFSStructure(fs.dsType, fs.workspace, dataset, version)
		if err != nil {
			msg := fmt.Sprintf("Failed to change FS to %v:%v: %v", dataset, version, err)
			logrus.Error(msg)
			return nodefs.NewDataFile([]byte(msg + "\n")), fuse.OK
		}
		//fs.lock.Lock()
		//defer fs.lock.Unlock()

		logrus.Infof("Changing dataset to %v:%v", dataset, version)
		fs.innerFS = newFS

		fs.dataset = dataset
		fs.version = version

		return nodefs.NewDataFile([]byte("dataset_new\n")), fuse.OK
	} else {
		return nodefs.NewDataFile([]byte("dataset_old\n")), fuse.OK
	}
}
