package webdav

import (
	"errors"
	"fmt"
	"os"

	"github.com/kuberlab/pluk/pkg/datasets"
	"github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/utils"
	"golang.org/x/net/context"
	"golang.org/x/net/webdav"
)

type FS struct {
	Dataset *datasets.Dataset
	Version string
}

func (*FS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return errors.New("Do not implemented.")
}

func (fs *FS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	// TODO: How to pass and apply flag and permissions? Currently read-only
	return io.NewChunkedFileFromRepo(fs.Dataset.Repo, fs.Version, name)
}

func (*FS) RemoveAll(ctx context.Context, name string) error {
	return errors.New("Do not implemented.")
}

func (*FS) Rename(ctx context.Context, oldName, newName string) error {
	return errors.New("Do not implemented.")
}

func (fs *FS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	chunked, err := io.NewChunkedFileFromRepo(fs.Dataset.Repo, fs.Version, name)
	if err != nil {
		return nil, err
	}
	return chunked.Stat()
}

func (fs *FS) fullPath(name string) string {
	return fmt.Sprintf("%v/%v/%v%v", utils.GitLocalDir(), fs.Dataset.Workspace, fs.Dataset.Name, name)
}
