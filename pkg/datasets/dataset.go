package datasets

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/db"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

type Dataset struct {
	*db.Dataset
	mgr          db.DataMgr
	FS           *plukio.ChunkedFileFS `json:"-"`
	MasterClient plukio.PlukClient     `json:"-"`
}

func (d *Dataset) Save(structure types.FileStructure, version string, comment string, create, publish, masterSave bool) error {
	if err := d.SaveFSToDB(structure, version); err != nil {
		return err
	}

	if utils.HasMasters() && masterSave {
		// TODO: decide whether it can go in async
		_ = d.MasterClient.SaveFileStructure(
			structure, d.Type, d.Workspace, d.Name, version, comment, create, publish,
		)
	}

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
	var fileSizeMap = make(map[string]int64)
	for _, f := range structure.Files {
		totalSize += f.Size
		fileSizeMap[f.Path] = f.Size
	}

	files, err := tx.ListFiles(
		db.File{
			DatasetType: d.Type,
			Workspace:   d.Workspace,
			Version:     version,
			DatasetName: d.Name,
		},
	)
	if err != nil {
		return err
	}
	for _, f := range files {
		if _, ok := fileSizeMap[f.Path]; !ok {
			totalSize += f.Size
		}
	}

	dsv := &db.DatasetVersion{
		Size:      totalSize,
		FileCount: int64(len(fileSizeMap)),
		Version:   version,
		Name:      d.Name,
		Workspace: d.Workspace,
		Type:      d.Type,
	}
	if err := SaveDatasetVersion(tx, dsv); err != nil {
		return err
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
		if err = SaveFile(tx, dsv, f); err != nil {
			return err
		}
	}
	return nil
}

func SaveDatasetVersion(tx db.DataMgr, dsv *db.DatasetVersion) error {
	dsvOld, err := tx.GetDatasetVersion(dsv.Type, dsv.Workspace, dsv.Name, dsv.Version)
	if err != nil {
		err = nil
		errD := tx.CreateDatasetVersion(dsv)
		if errD != nil {
			err = errD
			return err
		}
	} else if dsvOld.Deleted {
		// Recover it.
		dsvOld.Deleted = false
		dsvOld.Size = dsv.Size
		dsv.Deleted = false
		if dsv.Message != "" {
			dsvOld.Message = dsv.Message
		}
		if err = tx.RecoverDatasetVersion(dsvOld); err != nil {
			return err
		}
	} else {
		// Simple update
		dsvOld.Size = dsv.Size
		dsvOld.Editing = dsv.Editing || dsvOld.Editing
		dsv.Editing = dsvOld.Editing
		dsvOld.FileCount = dsv.FileCount
		if dsv.Message != "" {
			dsvOld.Message = dsv.Message
		}
		if _, err = tx.UpdateDatasetVersion(dsvOld); err != nil {
			return err
		}
	}
	return nil
}

func SaveFile(tx db.DataMgr, dsv *db.DatasetVersion, f *types.HashedFile) error {
	var fPath = f.Path
	var err error
	//if !strings.HasPrefix(fPath, "/") {
	//	fPath = "/" + fPath
	//}
	fileDB := &db.File{
		Size:        f.Size,
		Path:        fPath,
		Version:     dsv.Version,
		Workspace:   dsv.Workspace,
		DatasetName: dsv.Name,
		DatasetType: dsv.Type,
		Mode:        uint32(f.Mode),
	}
	if existing, errD := tx.GetFile(dsv.Workspace, dsv.Name, dsv.Type, fPath, dsv.Version); errD != nil {
		// Create
		err = tx.CreateFile(fileDB)
		if err != nil {
			return err
		}
	} else {
		// Need to clear unneeded chunks
		//
		if err = ClearExtraChunks(tx, dsv, existing, f); err != nil {
			return err
		}

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
	return nil
}

func ClearExtraChunks(tx db.DataMgr, dsv *db.DatasetVersion, existing *db.File, replacement *types.HashedFile) error {
	exChunks, err := tx.GetRawFiles(dsv.Type, dsv.Workspace, dsv.Name, dsv.Version, existing.Path)
	if err != nil {
		return err
	}
	candidates := make(map[string]db.RawFile)
	for _, exChunk := range exChunks {
		candidates[exChunk.Hash] = exChunk
	}
	// Delete all chunks from map which are in replacement file
	for _, newChunk := range replacement.Hashes {
		if _, ok := candidates[newChunk.Hash]; ok {
			delete(candidates, newChunk.Hash)
		}
	}

	// Delete candidates
	deleted := 0
	for _, candidate := range candidates {
		err := tx.DeleteFileChunk(candidate.FileID, candidate.ChunkID)
		if err != nil {
			return err
		}
		chunk := &db.Chunk{
			Hash: candidate.Hash,
			Size: candidate.ChunkSize,
			ID:   candidate.ChunkID,
		}
		if CheckAndDeleteChunk(tx, chunk) {
			deleted++
		}
	}
	if deleted != 0 {
		logrus.Infof("Deleted %v chunks.", deleted)
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
	_, err = d.mgr.GetDatasetVersion(d.Type, d.Workspace, d.Name, version)

	if err == nil {
		fs, err = d.GetFSFromDB(version)
	} else {
		if !utils.HasMasters() {
			return nil, fmt.Errorf(
				"Version %v not found in %v %v/%v.",
				version, d.Type, d.Workspace, d.Name,
			)
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
	fs, err := d.MasterClient.GetFSStructure(d.Type, d.Workspace, d.Name, version)

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
	return d.mgr.GetFS(d.Type, d.Workspace, d.Name, version)
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
	dsvs, err := d.mgr.ListDatasetVersions(
		db.DatasetVersion{Workspace: d.Workspace, Name: d.Name, Type: d.Type},
	)
	if err != nil {
		return nil, err
	}
	versionMap := make(map[string]types.Version)
	for _, dsv := range dsvs {
		versionMap[dsv.Version] = types.Version{
			Version:   dsv.Version,
			SizeBytes: dsv.Size,
			UpdatedAt: dsv.UpdatedAt,
			CreatedAt: dsv.CreatedAt,
			Editing:   dsv.Editing,
			Message:   dsv.Message,
			DType:     dsv.Type,
			FileCount: dsv.FileCount,
			Name:      dsv.Name,
			Workspace: dsv.Workspace,
		}
	}
	if utils.HasMasters() {
		vList, err := d.MasterClient.ListVersions(d.Type, d.Workspace, d.Name)
		if err != nil {
			return nil, err
		}

		for _, v := range vList.Versions {
			_, ok := versionMap[v.Version]
			if !ok {
				// Sync locally
				_ = d.mgr.CreateDatasetVersion(
					&db.DatasetVersion{
						Version:   v.Version,
						Editing:   v.Editing,
						Type:      v.DType,
						Size:      v.SizeBytes,
						Message:   v.Message,
						Name:      d.Name,
						Workspace: d.Workspace,
						FileCount: v.FileCount,
					},
				)
			}
			versionMap[v.Version] = v
		}
	}
	result := make([]types.Version, 0)
	for _, v := range versionMap {
		result = append(result, v)
	}

	sort.Sort(sort.Reverse(types.VersionArr(result)))
	return result, nil
}

func (d *Dataset) DeleteVersion(version string, force bool) error {
	dsv, err := d.mgr.GetDatasetVersion(d.Type, d.Workspace, d.Name, version)
	if err != nil {
		ok, _ := d.CheckVersion(version)
		if !ok {
			return errors.NewStatus(http.StatusNotFound, fmt.Sprintf("Version %v not found", version))
		}
	} else {
		dsv.Deleted = true
		if _, err = d.mgr.UpdateDatasetVersion(dsv); err != nil {
			return err
		}
	}

	if utils.HasMasters() && d.MasterClient != nil {
		_ = d.MasterClient.DeleteVersion(d.Type, d.Workspace, d.Name, version)
	}

	if force {
		utils.GCChan <- fmt.Sprintf("Clean version of %v/%v:%v", d.Workspace, d.Name, version)
	}

	return nil
}

func (d *Dataset) CommitVersion(version string, message string) (*db.DatasetVersion, error) {
	exist, err := d.CheckVersion(version)
	if !exist {
		return nil, fmt.Errorf("Version %v for dataset %v/%v doesn't exist.", version, d.Workspace, d.Name)
	} else if err != nil {
		return nil, err
	}

	return d.mgr.CommitVersion(d.Type, d.Workspace, d.Name, version, message)
}

func (d *Dataset) CloneVersionTo(target *Dataset, version, targetVersion, message string) (*db.DatasetVersion, error) {
	var err error
	tx := d.mgr.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	// Clean target version
	_ = DeleteFiles(
		tx, target.Type, target.Workspace,
		target.Name, targetVersion, "", false, false,
	)

	files, err := tx.ListFiles(
		db.File{
			Workspace:   d.Workspace,
			DatasetName: d.Name,
			Version:     version,
			DatasetType: d.Type,
		},
	)
	if err != nil {
		return nil, err
	}
	fileChunks, err := tx.ListRelatedChunks(d.Type, d.Workspace, d.Name, version)
	if err != nil {
		return nil, err
	}
	var fileChunksMap = make(map[uint][]*db.FileChunk)

	for _, fc := range fileChunks {
		if _, ok := fileChunksMap[fc.FileID]; ok {
			fileChunksMap[fc.FileID] = append(fileChunksMap[fc.FileID], fc)
		} else {
			fileChunksMap[fc.FileID] = []*db.FileChunk{fc}
		}
	}
	var totalSize int64 = 0

	// Create the same files with different workspace/dataset/versions
	for _, f := range files {
		totalSize += f.Size
		newF := &db.File{
			Workspace:   target.Workspace,
			Version:     targetVersion,
			DatasetName: target.Name,
			Size:        f.Size,
			Path:        f.Path,
			DatasetType: target.Type,
		}
		if existing, errD := tx.GetFile(target.Workspace, target.Name, target.Type, f.Path, targetVersion); errD != nil {
			// Create
			err = tx.CreateFile(newF)
			if err != nil {
				return nil, err
			}
		} else {
			// Update
			newF.ID = existing.ID
			if existing.Size != newF.Size {
				_, err = tx.UpdateFile(newF)
				if err != nil {
					return nil, err
				}
			}
		}

		// Create another chunk links for new file
		for _, oldFC := range fileChunksMap[f.ID] {
			newFC := &db.FileChunk{
				FileID:     newF.ID,
				ChunkID:    oldFC.ChunkID,
				ChunkIndex: oldFC.ChunkIndex,
			}
			if err = tx.CreateFileChunk(newFC); err != nil {
				return nil, err
			}
		}
	}

	sourceVersion, err := tx.GetDatasetVersion(d.Type, d.Workspace, d.Name, version)
	if err != nil {
		return nil, err
	}

	dsv := &db.DatasetVersion{
		Version:   targetVersion,
		Size:      totalSize,
		Name:      target.Name,
		Workspace: target.Workspace,
		Type:      target.Type,
		Editing:   true,
		Message:   message,
		FileCount: sourceVersion.FileCount,
	}
	err = SaveDatasetVersion(tx, dsv)
	return dsv, err
}

func (d *Dataset) CloneVersion(version, targetVersion, message string) (*db.DatasetVersion, error) {
	return d.CloneVersionTo(d, version, targetVersion, message)
}
