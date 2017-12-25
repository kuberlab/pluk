package webdav

import (
	"errors"
	"golang.org/x/net/context"
	"golang.org/x/net/webdav"
	"os"
)

type FS struct {
}

func (*FS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return errors.New("Do not implemented.")
}

func (*FS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	panic("implement me")
}

func (*FS) RemoveAll(ctx context.Context, name string) error {
	return errors.New("Do not implemented.")
}

func (*FS) Rename(ctx context.Context, oldName, newName string) error {
	return errors.New("Do not implemented.")
}

func (*FS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	panic("implement me")
}
