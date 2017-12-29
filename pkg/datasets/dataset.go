package datasets

import (
	"fmt"
	"path"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pacak/pkg/pacakimpl"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

const (
	Author        = "pluk"
	AuthorEmail   = "pluk@kuberlab.io"
	defaultBranch = "master"
)

type Dataset struct {
	types.Dataset
	git  pacakimpl.GitInterface
	FS   *plukio.ChunkedFileFS `json:"-"`
	Repo pacakimpl.PacakRepo   `json:"-"`
}

func (d *Dataset) Save(structure types.FileStructure, version string, comment string) error {
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

	if utils.HasMasters() {
		// TODO: decide whether it can go in async
		plukio.MasterClient.SaveFileStructure(structure, d.Workspace, d.Name, version)
	}

	return nil
}

func (d *Dataset) Download(version string, resp *restful.Response) error {
	// Build archive.
	_, err := d.GetFSStructure(version)
	if err != nil {
		return err
	}
	return WriteTarGz(d.FS, resp)
}

func (d *Dataset) GetFSStructure(version string) (fs *plukio.ChunkedFileFS, err error) {
	if d.Repo != nil {
		fs, err = d.getFSStructureFromRepo(version)
	} else {
		if !utils.HasMasters() {
			return nil, fmt.Errorf("Either the current instance has no masters or has corrupted repo")
		}
		fs, err = d.getFSStructureFromMaster(version)
	}

	if err != nil {
		return nil, err
	}

	fs.Prepare()
	d.FS = fs
	return fs, nil
}

func (d *Dataset) getFSStructureFromMaster(version string) (*plukio.ChunkedFileFS, error) {
	return plukio.MasterClient.GetFSStructure(d.Workspace, d.Name, version)
}

func (d *Dataset) getFSStructureFromRepo(version string) (*plukio.ChunkedFileFS, error) {
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
			Fname:    path.Base(chunked.Name),
			Dir:      gitFile.IsDir(),
		}
		if gitFile.IsDir() {
			chunked.Fstat.Fsize = 4096
		}
		fs.FS[gitFile.Name()] = chunked
	}
	return fs, err
}

func (d *Dataset) CheckVersion(version string) (bool, error) {
	if d.Repo == nil {
		versions, err := plukio.MasterClient.ListVersions(d.Workspace, d.Name)
		if err != nil {
			return false, err
		}
		for _, v := range versions.Versions {
			if v == version {
				return true, nil
			}
		}
		return false, errors.NewStatus(404, fmt.Sprintf("Version %v not found for dataset %v.", version, d.Name))
	}

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
	if d.Repo == nil {
		vList, err := plukio.MasterClient.ListVersions(d.Workspace, d.Name)
		if err != nil {
			return nil, err
		}
		return vList.Versions, nil
	}
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
