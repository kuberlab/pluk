package datasets

import (
	"fmt"
	"path"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pacak/pkg/pacakimpl"
	"github.com/kuberlab/pluk/pkg/db"
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

func (d *Dataset) Save(structure types.FileStructure, version string, comment string, create bool, masterSave bool) error {
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

	if utils.Exists(path.Join(utils.GitLocalDir(), d.Workspace, d.Name)) {
		logrus.Debugf("Cleaning data for %v/%v:%v...", d.Workspace, d.Name, version)
		if _, err := d.Repo.CleanPush(getCommitter(), "Clean FS before push", defaultBranch); err != nil {
			return err
		}
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

	if err = d.SaveFSToDB(structure, version); err != nil {
		return err
	}

	if utils.HasMasters() && masterSave {
		// TODO: decide whether it can go in async
		plukio.MasterClient.SaveFileStructure(structure, d.Workspace, d.Name, version, create)
	}

	return nil
}

func (d *Dataset) SaveFSToDB(structure types.FileStructure, version string) (err error) {
	repoPath := path.Join(utils.GitLocalDir(), d.Workspace, d.Name)

	tx := db.DbMgr.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	for _, f := range structure.Files {
		var fPath = f.Path
		//if !strings.HasPrefix(fPath, "/") {
		//	fPath = "/" + fPath
		//}
		fileDB := &db.File{
			Size:           int64(f.Size),
			Path:           fPath,
			RepositoryPath: repoPath,
			Version:        version,
		}
		if existing, errD := tx.GetFile(fPath, repoPath, version); errD != nil {
			// Create
			err = tx.CreateFile(fileDB)
			if err != nil {
				return err
			}
		} else {
			// Update
			fileDB.ID = existing.ID
			if existing.Size != fileDB.Size {
				_, err = tx.UpdateFile(fileDB)
				if err != nil {
					return err
				}
			}
		}

		for _, hash := range f.Hashes {
			chunk := &db.Chunk{Hash: hash}
			if eChunk, errD := tx.GetChunk(hash); errD != nil {
				if err = tx.CreateChunk(chunk); err != nil {
					return err
				}
			} else {
				chunk.ID = eChunk.ID
			}
			// Create connection
			fileChunk := &db.FileChunk{ChunkID: chunk.ID, FileID: fileDB.ID}

			if _, errD := tx.GetFileChunk(fileDB.ID, chunk.ID); errD != nil {
				if err = tx.CreateFileChunk(fileChunk); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d *Dataset) Download(resp *restful.Response) error {
	return WriteTarGz(d.FS, resp)
}

func (d *Dataset) GetFSStructure(version string) (fs *plukio.ChunkedFileFS, err error) {
	var versions []string
	var found = false
	if d.Repo != nil {
		versions, err = d.Repo.TagList()
		if err != nil {
			return nil, err
		}
		for _, v := range versions {
			if v == version {
				found = true
			}
		}
	}

	if d.Repo != nil && found {
		fs, err = d.GetFSStructureFromRepo(version)
	} else {
		if !utils.HasMasters() {
			return nil, fmt.Errorf("Either the current instance has no masters or version does not exist.")
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
	fs, err := plukio.MasterClient.GetFSStructure(d.Workspace, d.Name, version)

	if err != nil {
		return nil, err
	}
	go func() {
		if err := d.SaveFSLocally(fs, version); err != nil {
			logrus.Errorf("Unable save FS: %v", err)
		}
	}()
	return fs, err
}

func (d *Dataset) SaveFSLocally(src *plukio.ChunkedFileFS, version string) error {
	d.InitRepo(true)
	dest := types.FileStructure{}
	for _, f := range src.FS {
		if f.Fstat.Dir {
			continue
		}
		file := types.HashedFile{
			Path:   strings.TrimPrefix(f.Name, "/"),
			Size:   uint64(f.Size),
			Hashes: make([]string, 0),
		}
		for _, chunkPath := range f.Chunks {
			hash := utils.GetHashFromPath(chunkPath.Path)
			file.Hashes = append(file.Hashes, hash)
		}
		dest.Files = append(dest.Files, &file)
	}

	return d.Save(dest, version, "", false, false)
}

func (d *Dataset) GetFSStructureFromRepo(version string) (*plukio.ChunkedFileFS, error) {
	gitFiles, err := d.Repo.ListFilesAtRev(version)
	if err != nil {
		return nil, err
	}

	return plukio.InitChunkedFSFromRepo(d.Repo, version, gitFiles)
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
