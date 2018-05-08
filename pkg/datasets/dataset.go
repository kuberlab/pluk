package datasets

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/db"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

const (
	Author      = "pluk"
	AuthorEmail = "pluk@kuberlab.io"
)

type Dataset struct {
	*db.Dataset
	mgr          db.DataMgr
	FS           *plukio.ChunkedFileFS `json:"-"`
	MasterClient plukio.PlukClient     `json:"-"`
}

func (d *Dataset) Save(structure types.FileStructure, version string, comment string, create, publish, masterSave bool) error {
	logrus.Infof("Saving data for %v/%v:%v...", d.Workspace, d.Name, version)

	if err := d.SaveFSToDB(structure, version); err != nil {
		return err
	}

	if utils.HasMasters() && masterSave {
		// TODO: decide whether it can go in async
		d.MasterClient.SaveFileStructure(structure, d.Workspace, d.Name, version, create, publish)
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

	var totalSize int64 = 0
	for _, f := range structure.Files {
		totalSize += f.Size
	}

	dsv, err := tx.GetDatasetVersion(d.Workspace, d.Name, version)
	if err != nil {
		err = nil
		errD := tx.CreateDatasetVersion(&db.DatasetVersion{
			Name:      d.Name,
			Workspace: d.Workspace,
			Version:   version,
			Size:      totalSize,
		})
		if errD != nil {
			err = errD
			return
		}
	} else if dsv.Deleted {
		// Recover it.
		dsv.Deleted = false
		dsv.Size = totalSize
		if err = tx.RecoverDatasetVersion(dsv); err != nil {
			return err
		}
	} else {
		// Simple update
		dsv.Size = totalSize
		if _, err = tx.UpdateDatasetVersion(dsv); err != nil {
			return err
		}
	}

	// For batch insert:
	// 1. Delete all related files of workspace + dataset_name + version
	// 2. Generate insert query for files
	// 3. Batch insert files
	// 4. Create chunks if needed (one by one only), collect their ids and index
	// 5. Get all file_ids for workspace + dataset + version
	// 6. Generate insert query for file_chunks connections
	// 7. Batch insert file_chunks

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
				if eChunk.Size != chunk.Size {
					_, err = tx.UpdateChunk(chunk)
					if err != nil {
						return err
					}
				}
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
	return WriteTar(d.FS, resp)
}

func (d *Dataset) TarSize() (int64, error) {
	var size int64 = 0
	err := d.FS.Walk("/", func(path string, f *plukio.ChunkedFile, err error) error {
		name := strings.TrimPrefix(path, "/")
		if strings.HasPrefix(name, ".") || path == "/" {
			return nil
		}

		if f.Fstat.IsDir() {
			// Directory size
			// size += 4096
			return nil
		}
		// Header size
		size += 512

		// File size padded to 512
		size += f.Size
		if f.Size%512 != 0 {
			size += 512 - f.Size%512
		}
		return nil
	})
	// 2 end blocks
	size += 512 * 2
	return size, err
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
	fs, err := d.MasterClient.GetFSStructure(d.Workspace, d.Name, version)

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
	err := src.Walk("/", func(path string, f *plukio.ChunkedFile, err error) error {
		if f.Fstat.IsDir() {
			return nil
		}
		file := types.HashedFile{
			Path:   strings.TrimPrefix(path, "/"),
			Size:   f.Size,
			Hashes: make([]types.Hash, 0),
		}
		for _, chunk := range f.Chunks {
			hash := utils.GetHashFromPath(chunk.Path)
			file.Hashes = append(file.Hashes, types.Hash{Hash: hash, Size: chunk.Size})
		}
		dest.Files = append(dest.Files, &file)
		return nil
	})
	if err != nil {
		return err
	}

	return d.Save(dest, version, "", false, false, false)
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
		if v.Version == version {
			return true, nil
		}
	}
	return false, nil
}

func (d *Dataset) Versions() ([]types.Version, error) {
	dsvs, err := d.mgr.ListDatasetVersions(db.DatasetVersion{Workspace: d.Workspace, Name: d.Name})
	if err != nil {
		return nil, err
	}
	versionMap := make(map[string]types.Version)
	for _, dsv := range dsvs {
		versionMap[dsv.Version] = types.Version{Version: dsv.Version, SizeBytes: dsv.Size}
	}
	if utils.HasMasters() {
		vList, err := d.MasterClient.ListVersions(d.Workspace, d.Name)
		if err != nil {
			return nil, err
		}

		for _, v := range vList.Versions {
			versionMap[v.Version] = v
		}
	}
	result := make([]types.Version, 0)
	for _, v := range versionMap {
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
