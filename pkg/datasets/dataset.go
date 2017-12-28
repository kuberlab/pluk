package datasets

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pacak/pkg/pacakimpl"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/utils"
)

const (
	Author        = "pluk"
	AuthorEmail   = "pluk@kuberlab.io"
	defaultBranch = "master"
)

type FileStructure struct {
	Files []*HashedFile `json:"files"`
}

type HashedFile struct {
	Path     string      `json:"path"`
	Size     uint64      `json:"size"`
	Hashes   []string    `json:"hashes"`
	Mode     os.FileMode `json:"mode"`
	ModeTime time.Time   `json:"mode_time"`
}

type Dataset struct {
	git       pacakimpl.GitInterface
	Repo      pacakimpl.PacakRepo `json:"-"`
	Workspace string              `json:"workspace"`
	Name      string              `json:"name"`
}

func (d *Dataset) Save(structure FileStructure, version string, comment string) error {
	// Make absolute path for hashes and build gitFiles
	files := make([]pacakimpl.GitFile, 0)
	for _, f := range structure.Files {
		paths := make([]string, 0)
		for _, h := range f.Hashes {
			filePath := utils.GetHashedFilename(h)
			paths = append(paths, filePath)
		}
		// Virtual file structure:
		// <size (uint64)>
		// <chunk path1>
		// <chunk path2>
		// ..
		// <chunk pathN>
		//
		content := fmt.Sprintf("%v\n%v", f.Size, strings.Join(paths, "\n"))
		files = append(files, pacakimpl.GitFile{Path: f.Path, Data: []byte(content)})
	}
	logrus.Infof("Saving data for %v/%v:%v...", d.Workspace, d.Name, version)

	commit, err := d.Repo.Save(getCommitter(), buildMessage(version, comment), defaultBranch, defaultBranch, files)
	if err != nil {
		return err
	}
	logrus.Infof("Saved as commit %v.", commit)

	if err = d.Repo.PushTag(version, commit, true); err != nil {
		return err
	}
	logrus.Infof("Created tag %v.", version)

	return nil
}

func CheckChunk(hash string) bool {
	filePath := utils.GetHashedFilename(hash)
	_, err := os.Stat(filePath)
	return err == nil
}

func GetChunk(hash string) (io.ReadCloser, error) {
	filePath := utils.GetHashedFilename(hash)
	return os.Open(filePath)
}

func SaveChunk(hash string, data io.ReadCloser) error {
	filePath := utils.GetHashedFilename(hash)

	splitted := strings.Split(filePath, "/")
	baseDir := splitted[:len(splitted)-1]

	if err := os.MkdirAll(strings.Join(baseDir, "/"), os.ModePerm); err != nil {
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	logrus.Debugf("Created %v", filePath)

	defer file.Close()

	written, err := io.Copy(file, data)
	if err != nil {
		return err
	}
	data.Close()

	logrus.Debugf("Written %v bytes.", written)
	return nil
}

func (d *Dataset) Download(version string, resp *restful.Response) error {
	// Build archive.
	return WriteTarGz(d.Repo, version, resp)
}

func (d *Dataset) GetFSStructure(version string) (*plukio.ChunkedFileFS, error) {
	gitFiles, err := d.Repo.ListFilesAtRev(version)
	if err != nil {
		return nil, err
	}

	fs := &plukio.ChunkedFileFS{FS: make(map[string]*plukio.ChunkedFile)}
	for _, gitFile := range gitFiles {
		chunked, err := plukio.NewInternalChunked(d.Repo, version, gitFile.Name())
		if err != nil {
			return nil, err
		}
		chunked.Fstat = &plukio.ChunkedFileInfo{
			FmodTime: gitFile.ModTime(),
			Fmode:    gitFile.Mode(),
			Fsize:    chunked.Size,
			Fname:    chunked.Name,
			Dir:      gitFile.IsDir(),
		}
		if gitFile.IsDir() {
			chunked.Fstat.Fsize = 4096
		}
		fs.FS[gitFile.Name()] = chunked
	}

	return fs, nil
}

func (d *Dataset) CheckVersion(version string) (bool, error) {
	if !d.Repo.IsTagExists(version) {
		return false, errors.NewStatus(404, fmt.Sprintf("Version %v not found for dataset %v.", version, d.Name))
	}
	return true, nil
}

func (d *Dataset) CheckoutVersion(version string) error {
	logrus.Infof("Checkout tag %v.", version)

	if !d.Repo.IsTagExists(version) {
		return errors.NewStatus(404, fmt.Sprintf("Version %v not found for dataset %v.", version, d.Name))
	}

	return d.Repo.Checkout(version)
}

func (d *Dataset) Versions() ([]string, error) {
	return d.Repo.TagList()
}

func (d *Dataset) Delete() error {
	repo := fmt.Sprintf("%v/%v", d.Workspace, d.Name)

	return d.git.DeleteRepository(repo)
}

func (d *Dataset) DeleteVersion(version string) error {
	return d.Repo.DeleteTag(version)
}

func (d *Dataset) InitRepo(create bool) error {
	repo := fmt.Sprintf("%v/%v", d.Workspace, d.Name)
	pacakRepo, err := d.git.GetRepository(repo)
	if err != nil {
		if !create {
			return err
		}
		if err = d.git.InitRepository(getCommitter(), repo, []pacakimpl.GitFile{}); err != nil {
			return err
		}
	}

	if pacakRepo == nil {
		pacakRepo, err = d.git.GetRepository(repo)
		if err != nil {
			return err
		}
	}
	d.Repo = pacakRepo
	return nil
}

func buildMessage(version, comment string) string {
	return fmt.Sprintf("Version: %v\nComment: %v", version, comment)
}
