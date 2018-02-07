package datasets

import (
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/db"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
	"net/http"
)

const (
	Author      = "pluk"
	AuthorEmail = "pluk@kuberlab.io"
)

type Dataset struct {
	*db.Dataset
	mgr db.DataMgr
	FS  *plukio.ChunkedFileFS `json:"-"`
}

func (d *Dataset) Save(structure types.FileStructure, version string, comment string, create bool, masterSave bool) error {
	logrus.Infof("Saving data for %v/%v:%v...", d.Workspace, d.Name, version)

	if err := d.SaveFSToDB(structure, version); err != nil {
		return err
	}

	if utils.HasMasters() && masterSave {
		// TODO: decide whether it can go in async
		plukio.MasterClient.SaveFileStructure(structure, d.Workspace, d.Name, version, create)
	}
	logrus.Infof("Done saving %v/%v:%v.", d.Workspace, d.Name, version)

	return nil
}

func (d *Dataset) SaveFSToDB(structure types.FileStructure, version string) (err error) {
	tx := db.DbMgr.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	dsv, err := tx.GetDatasetVersion(d.Workspace, d.Name, version)
	if err != nil {
		err = nil
		errD := tx.CreateDatasetVersion(&db.DatasetVersion{Name: d.Name, Workspace: d.Workspace, Version: version})
		if errD != nil {
			err = errD
			return
		}
	} else if dsv.Deleted {
		// Recover it.
		dsv.Deleted = false
		if err = tx.RecoverDatasetVersion(dsv); err != nil {
			return err
		}
	}

	for _, f := range structure.Files {
		var fPath = f.Path
		//if !strings.HasPrefix(fPath, "/") {
		//	fPath = "/" + fPath
		//}
		fileDB := &db.File{
			Size:        int64(f.Size),
			Path:        fPath,
			Version:     version,
			Workspace:   d.Workspace,
			DatasetName: d.Name,
		}
		if existing, errD := tx.GetFile(d.Workspace, d.Name, fPath, version); errD != nil {
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

		for i, hash := range f.Hashes {
			chunk := &db.Chunk{Hash: hash.Hash, Size: hash.Size}
			if eChunk, errD := tx.GetChunk(hash.Hash); errD != nil {
				if err = tx.CreateChunk(chunk); err != nil {
					return err
				}
			} else {
				chunk.ID = eChunk.ID
			}
			// Create connection
			fileChunk := &db.FileChunk{ChunkID: chunk.ID, FileID: fileDB.ID, ChunkIndex: uint(i)}

			if _, errD := tx.GetFileChunk(fileDB.ID, chunk.ID, i); errD != nil {
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
	_, err = d.mgr.GetDatasetVersion(d.Workspace, d.Name, version)

	if err == nil {
		fs, err = d.GetFSFromDB(version)
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
	dest := types.FileStructure{}
	for _, f := range src.FS {
		if f.Fstat.Dir {
			continue
		}
		file := types.HashedFile{
			Path:   strings.TrimPrefix(f.Name, "/"),
			Size:   f.Size,
			Hashes: make([]types.Hash, 0),
		}
		for _, chunk := range f.Chunks {
			hash := utils.GetHashFromPath(chunk.Path)
			file.Hashes = append(file.Hashes, types.Hash{Hash: hash, Size: chunk.Size})
		}
		dest.Files = append(dest.Files, &file)
	}

	return d.Save(dest, version, "", false, false)
}

func (d *Dataset) GetFSFromDB(version string) (*plukio.ChunkedFileFS, error) {
	return d.mgr.GetFS(d.Workspace, d.Name, version)
}

func (d *Dataset) CheckVersion(version string) (bool, error) {
	versions, err := d.Versions()
	if err != nil {
		return false, err
	}
	for _, v := range versions {
		if v == version {
			return true, nil
		}
	}
	return false, nil
}

func (d *Dataset) Versions() ([]string, error) {
	dsvs, err := d.mgr.ListDatasetVersions(db.DatasetVersion{Workspace: d.Workspace, Name: d.Name})
	if err != nil {
		return nil, err
	}
	versionMap := make(map[string]bool)
	for _, dsv := range dsvs {
		versionMap[dsv.Version] = true
	}
	if utils.HasMasters() {
		vList, err := plukio.MasterClient.ListVersions(d.Workspace, d.Name)
		if err != nil {
			return nil, err
		}

		for _, v := range vList.Versions {
			versionMap[v] = true
		}
	}
	result := make([]string, 0)
	for v := range versionMap {
		result = append(result, v)
	}

	return result, nil
}

func (d *Dataset) DeleteVersion(version string) error {
	dsv, err := d.mgr.GetDatasetVersion(d.Workspace, d.Name, version)
	if err != nil {
		return errors.NewStatus(http.StatusNotFound, fmt.Sprintf("Version %v not found", version))
	}
	dsv.Deleted = true
	if _, err = d.mgr.UpdateDatasetVersion(dsv); err != nil {
		return err
	}
	return nil
}
