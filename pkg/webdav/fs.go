package webdav

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/kuberlab/pluk/pkg/datasets"
	"github.com/kuberlab/pluk/pkg/utils"
	"golang.org/x/net/context"
	"golang.org/x/net/webdav"
)

type FS struct {
	Dataset *datasets.Dataset
	Version string
	cache   *utils.RequestCache
}

func NewFS(dataset *datasets.Dataset, version string) webdav.FileSystem {
	return &FS{Dataset: dataset, Version: version, cache: utils.NewRequestCache()}
}

func (*FS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return errors.New("Do not implemented.")
}

func (fs *FS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	// TODO: How to pass and apply flag and permissions? Currently read-only
	if name != "/" {
		name = strings.TrimSuffix(name, "/")
	}
	f, ok := fs.Dataset.FS.FS[name]
	if !ok {
		return nil, fmt.Errorf("%v: No such file or directory", name)
	}
	return f, nil
}

func (*FS) RemoveAll(ctx context.Context, name string) error {
	return errors.New("Do not implemented.")
}

func (*FS) Rename(ctx context.Context, oldName, newName string) error {
	return errors.New("Do not implemented.")
}

func (fs *FS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	if name != "/" {
		name = strings.TrimSuffix(name, "/")
	}
	f, ok := fs.Dataset.FS.FS[name]
	if !ok {
		return nil, fmt.Errorf("%v: No such file or directory", name)
	}
	return f.Fstat, nil
}
