package datasets

import (
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/db"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

const (
	limit      = 100
	chunkLimit = 250
)

type Dataset struct {
	*db.Dataset
	mgr          db.DataMgr
	FS           *plukio.ChunkedFileFS `json:"-"`
	MasterClient plukio.PlukClient     `json:"-"`
}

func (d *Dataset) Save(structure types.FileStructure,
	version string, comment string, create, publish, editing, masterSave bool) error {
	if err := d.SaveFSToDB(structure, version); err != nil {
		return err
	}

	if utils.HasMasters() && masterSave {
		// TODO: decide whether it can go in async
		_ = d.MasterClient.SaveFileStructure(
			structure, d.Type, d.Workspace, d.Name, version,
			types.SaveOpts{Comment: comment, Create: create, Publish: publish, Editing: editing},
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
			fileSizeMap[f.Path] = f.Size
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

	endCh := make(chan error)
	fileChannel := make(chan *types.HashedFile)
	lock := &sync.RWMutex{}
	go func(structure types.FileStructure) {
		for _, f := range structure.Files {
			if err = CheckAndQueueFile(tx, dsv, f, fileChannel, lock); err != nil {
				endCh <- err
				return
			}
		}
		endCh <- nil

	}(structure)

	return receiveFileToSave(tx, dsv, fileChannel, endCh, lock)
}

func receiveFileToSave(tx db.DataMgr, dsv *db.DatasetVersion, fileChannel chan *types.HashedFile, endCh chan error, lock *sync.RWMutex) error {
	buffer := make([]*db.RawFile, 0)
	bufFiles := make([]*db.File, 0)
	fileMap := make(map[string][]*db.RawFile)

	tryFlushBuffer := func(chunk *db.RawFile, force bool) error {
		if chunk != nil {
			buffer = append(buffer, chunk)
		}
		if len(buffer) >= chunkLimit || force {
			lock.Lock()
			TriggerDeleteChunks(tx)
			err := createConnections(tx, buffer)
			lock.Unlock()
			buffer = nil
			if err != nil {
				return err
			}
		}
		return nil
	}

	flushFileBuffer := func() error {
		if len(bufFiles) == 0 {
			return nil
		}
		lock.Lock()
		// Create batch of files
		err := tx.CreateFiles(bufFiles)
		lock.Unlock()

		if err != nil {
			logrus.Error(err)
			return err
		}

		for _, bufFile := range bufFiles {
			raws := fileMap[bufFile.Path]
			for _, raw := range raws {
				raw.FileID = bufFile.ID
				if err = tryFlushBuffer(raw, false); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for {
		select {
		case f := <-fileChannel:
			//
			fileDB := &db.File{
				Size:        f.Size,
				Path:        f.Path,
				Version:     dsv.Version,
				Workspace:   dsv.Workspace,
				DatasetName: dsv.Name,
				DatasetType: dsv.Type,
				Mode:        uint32(f.Mode),
			}
			bufFiles = append(bufFiles, fileDB)
			for i, h := range f.Hashes {
				chunk := &db.RawFile{
					ChunkSize:  h.Size,
					Hash:       h.Hash,
					ChunkIndex: uint(i),
					Path:       f.Path,
					Version:    h.Version,
				}
				if _, ok := fileMap[f.Path]; !ok {
					fileMap[f.Path] = []*db.RawFile{chunk}
				} else {
					fileMap[f.Path] = append(fileMap[f.Path], chunk)
				}
			}
			if len(bufFiles) >= limit {
				if err := flushFileBuffer(); err != nil {
					close(fileChannel)
					return err
				}

				bufFiles = nil
				fileMap = make(map[string][]*db.RawFile)
			}

		case err := <-endCh:
			//
			if err != nil {
				close(fileChannel)
				return err
			}
			if err = flushFileBuffer(); err != nil {
				return err
			}
			if err = tryFlushBuffer(nil, true); err != nil {
				return err
			}
			fileMap = nil
			buffer = nil
			bufFiles = nil
			return err
		}
	}
}

func createConnections(mgr db.DataMgr, raws []*db.RawFile) error {
	// Create and get chunk ids
	if len(raws) == 0 {
		return nil
	}
	err := mgr.CreateChunks(raws)
	if err != nil {
		return err
	}
	// Create file_chunks
	fileChunks := make([]*db.FileChunk, len(raws))
	for i, raw := range raws {
		fileChunks[i] = &db.FileChunk{ChunkID: raw.ChunkID, FileID: raw.FileID, ChunkIndex: raw.ChunkIndex}
	}
	return mgr.CreateFileChunks(fileChunks)
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

func CheckAndQueueFile(tx db.DataMgr, dsv *db.DatasetVersion, f *types.HashedFile,
	fileChannel chan *types.HashedFile, lock *sync.RWMutex) (err error) {
	lock.Lock()
	if _, err = tx.GetFile(dsv.Workspace, dsv.Name, dsv.Type, f.Path, dsv.Version); err == nil {
		// Need to clear unneeded chunks
		if err = ClearExtraChunks(tx, dsv, f.Path, f); err != nil {
			lock.Unlock()
			return err
		}
	}
	lock.Unlock()
	err = nil

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Possible DB error.")
		}
	}()
	fileChannel <- f

	return err
}

func ClearExtraChunks(tx db.DataMgr, dsv *db.DatasetVersion, path string, replacement *types.HashedFile) error {
	exChunks, err := tx.GetRawFiles(dsv.Type, dsv.Workspace, dsv.Name, dsv.Version, path, "", true)
	if err != nil {
		return err
	}
	if len(exChunks) == 0 {
		return nil
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
	//deleted := 0
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
		CheckAndDeleteChunk(tx, chunk)
	}
	//deleted := TriggerDeleteChunks(tx)
	//if deleted != 0 {
	//	logrus.Infof("Deleted %v chunks.", deleted)
	//}
	return nil
}

func (d *Dataset) Download(resp *restful.Response) error {
	return WriteTar(d.FS.Clone(), resp)
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > unicode.MaxASCII {
			return false
		}
	}
	return true
}

func (d *Dataset) TarSize() (int64, error) {
	var size int64 = 0
	err := d.FS.Walk("/", func(path string, f *plukio.ChunkedFile, err error) error {
		name := path
		// Inline strings.TrimPrefix(): more performance
		if len(path) >= len("/") && path[:1] == "/" {
			name = path[1:]
		}

		// Inlining function: more performance
		//if strings.HasPrefix(name, ".") || path == "/" {
		if (len(name) >= len(".") && name[:1] == ".") || path == "/" {
			return nil
		}

		if f.Dir {
			// Directory size
			// size += 4096
			return nil
		}
		if !isASCII(name) {
			size += 1024
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

func (d *Dataset) GetFSStructure(version string, filters ...string) (fs *plukio.ChunkedFileFS, err error) {
	_, err = d.mgr.GetDatasetVersion(d.Type, d.Workspace, d.Name, version)

	if err == nil {
		fs, err = d.GetFSFromDB(version, filters...)
		if err == nil && len(fs.Dirs) == 0 && len(fs.Files) == 0 && utils.HasMasters() {
			// Empty FS in the DB; need to get FS from master.
			fs, err = d.getFSStructureFromMaster(version, filters...)
		}
	} else {
		if !utils.HasMasters() {
			return nil, fmt.Errorf(
				"Version %v not found in %v %v/%v.",
				version, d.Type, d.Workspace, d.Name,
			)
		}
		fs, err = d.getFSStructureFromMaster(version, filters...)
	}

	if err != nil {
		return nil, err
	}

	fs.Prepare()
	d.FS = fs
	return fs, nil
}

func (d *Dataset) getFSStructureFromMaster(version string, filters ...string) (*plukio.ChunkedFileFS, error) {
	var filter = ""
	if len(filters) > 0 {
		filter = filters[0]
	}
	fs, err := d.MasterClient.GetFSStructure(d.Type, d.Workspace, d.Name, version, filter)

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
	dest := types.FileStructure{
		Files: make([]*types.HashedFile, 0),
	}
	err := src.Walk("/", func(path string, f *plukio.ChunkedFile, err error) error {
		if f.Dir {
			return nil
		}
		file := types.HashedFile{
			Path:     strings.TrimPrefix(path, "/"),
			Size:     f.Size,
			Hashes:   make([]types.Hash, 0),
			Mode:     os.FileMode(f.Mode),
			ModeTime: f.ModTime,
		}
		for _, chunk := range f.Chunks {
			hash, version := utils.GetHashFromPath(chunk.Path)
			file.Hashes = append(file.Hashes, types.Hash{Hash: hash, Size: chunk.Size, Version: version})
		}
		dest.Files = append(dest.Files, &file)
		return nil
	})
	if err != nil {
		return err
	}

	return d.Save(dest, version, "", false, false, false, false)
}

func (d *Dataset) GetFSFromDB(version string, filters ...string) (*plukio.ChunkedFileFS, error) {
	var filter = ""
	if len(filters) > 0 {
		filter = filters[0]
	}
	return d.mgr.GetFS(d.Type, d.Workspace, d.Name, version, filter)
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
	if utils.HasMasters() && d.MasterClient != nil {
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

type ClonedFile struct {
	oldID   uint
	newFile *db.File
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

	// Create the same files with different workspace/dataset/versions
	endChan := make(chan error)
	fcChan := make(chan *db.FileChunk)
	fileBuf := make([]*ClonedFile, 0)
	fcBuf := make([]*db.FileChunk, 0)

	flushFileBuffer := func() error {
		files := make([]*db.File, len(fileBuf))
		for i, cloned := range fileBuf {
			files[i] = cloned.newFile
		}
		err := tx.CreateFiles(files)
		if err != nil {
			return err
		}
		for _, cloned := range fileBuf {
			// Create another chunk links for new cloned
			for _, oldFC := range fileChunksMap[cloned.oldID] {
				newFC := &db.FileChunk{
					FileID:     cloned.newFile.ID,
					ChunkID:    oldFC.ChunkID,
					ChunkIndex: oldFC.ChunkIndex,
				}
				fcChan <- newFC
			}
		}
		fileBuf = nil
		return nil
	}

	flushChunksBuffer := func() error {
		if len(fcBuf) == 0 {
			return nil
		}
		err := tx.CreateFileChunks(fcBuf)
		if err != nil {
			return err
		}
		fcBuf = nil
		return nil
	}

	go func() {
		defer func() {
			recover()
		}()
		for i, f := range files {
			newF := &db.File{
				Workspace:   target.Workspace,
				Version:     targetVersion,
				DatasetName: target.Name,
				Size:        f.Size,
				Path:        f.Path,
				DatasetType: target.Type,
				Mode:        f.Mode,
			}
			cloned := &ClonedFile{newFile: newF, oldID: f.ID}
			fileBuf = append(fileBuf, cloned)
			last := i == len(files)-1
			if len(fileBuf) >= limit || last {
				if err = flushFileBuffer(); err != nil {
					endChan <- err
				}
			}
		}

		fileBuf = nil
		endChan <- nil
	}()

	completed := false
	for {
		select {
		case fc := <-fcChan:
			fcBuf = append(fcBuf, fc)
			if len(fcBuf) >= chunkLimit {
				if err = flushChunksBuffer(); err != nil {
					return nil, err
				}
			}
		case err = <-endChan:
			//
			close(fcChan)
			if err != nil {
				return nil, err
			}

			if err = flushChunksBuffer(); err != nil {
				return nil, err
			}
			completed = true
			break
		}
		if completed {
			break
		}
	}

	sourceVersion, err := tx.GetDatasetVersion(d.Type, d.Workspace, d.Name, version)
	if err != nil {
		return nil, err
	}

	dsv := &db.DatasetVersion{
		Version:   targetVersion,
		Size:      sourceVersion.Size,
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
