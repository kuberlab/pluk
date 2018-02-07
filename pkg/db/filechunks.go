package db

import (
	"fmt"
	"path"
	"strings"

	libtypes "github.com/kuberlab/lib/pkg/types"
	"github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/utils"
)

type FileChunkMgr interface {
	CreateFileChunk(file *FileChunk) error
	GetFileChunk(fileID uint, chunkID uint, index int) (*FileChunk, error)
	ListFileChunks(filter FileChunk) ([]*FileChunk, error)
	DeleteFileChunk(fileID, chunkID uint) error
	GetFS(workspace, dataset, version string) (*io.ChunkedFileFS, error)
	DeleteRelatedFiles(workspace, dataset, version string) (int64, error)
	ListRelatedChunks(workspace, dataset, version string) ([]*FileChunk, error)
}

type FileChunk struct {
	FileID     uint `gorm:"index:file_id" json:"file_id"`
	ChunkID    uint `gorm:"index:chunk_id" json:"chunk_id"`
	ChunkIndex uint `json:"chunk_index"`
}

func (mgr *DatabaseMgr) CreateFileChunk(file *FileChunk) error {
	return mgr.db.Create(file).Error
}

func (mgr *DatabaseMgr) GetFileChunk(fileID uint, chunkID uint, index int) (*FileChunk, error) {
	var fileChunk = FileChunk{}
	filter := FileChunk{FileID: fileID, ChunkID: chunkID}
	if index != -1 {
		filter.ChunkIndex = uint(index)
	}
	err := mgr.db.First(&fileChunk, filter).Error
	return &fileChunk, err
}

func (mgr *DatabaseMgr) ListFileChunks(filter FileChunk) ([]*FileChunk, error) {
	var fileChunks = make([]*FileChunk, 0)
	err := mgr.db.Find(&fileChunks, filter).Error
	return fileChunks, err
}

func (mgr *DatabaseMgr) DeleteFileChunk(fileID, chunkID uint) error {
	return mgr.db.Delete(FileChunk{}, FileChunk{FileID: fileID, ChunkID: chunkID}).Error
}

func (mgr *DatabaseMgr) ListRelatedChunks(workspace, dataset, version string) ([]*FileChunk, error) {
	/*
		SELECT chunk_id FROM file_chunks
		INNER JOIN files ON
		  files.workspace='kuberlab-demo'
		  AND files.dataset_name='heavy'
		  AND files.version='1.0.0'
		  AND file_chunks.file_id=files.id;
	*/
	conditions := []string{
		fmt.Sprintf("files.workspace='%v'", workspace),
		fmt.Sprintf("files.dataset_name='%v'", dataset),
		"file_chunks.file_id=files.id",
	}
	if version != "" {
		conditions = append(conditions, fmt.Sprintf("files.version='%v'", version))
	}
	join := fmt.Sprintf("INNER JOIN files ON %v", strings.Join(conditions, " AND "))

	fileChunks := make([]*FileChunk, 0)
	err := mgr.db.
		Table("file_chunks").
		Select("distinct chunk_id").
		Joins(join).
		Scan(&fileChunks).Error

	return fileChunks, err
}

func (mgr *DatabaseMgr) DeleteRelatedFiles(workspace, dataset, version string) (int64, error) {
	conditions := []string{
		fmt.Sprintf("files.workspace='%v'", workspace),
		fmt.Sprintf("files.dataset_name='%v'", dataset),
	}
	if version != "" {
		conditions = append(conditions, fmt.Sprintf("files.version='%v'", version))
	}
	condition := strings.Join(conditions, " AND ")
	sqlDeleteRelation := fmt.Sprintf(
		"DELETE FROM file_chunks where file_id in ("+
			"SELECT id from files where %v)", condition,
	)

	err := mgr.db.Exec(sqlDeleteRelation).Error
	if err != nil {
		return 0, err
	}

	sqlDeleteFiles := fmt.Sprintf("DELETE FROM files where %v", condition)
	db := mgr.db.Exec(sqlDeleteFiles)

	return db.RowsAffected, db.Error
}

type rawFile struct {
	FileID     uint
	ChunkID    uint
	Path       string
	FileSize   int64
	ChunkSize  int64
	Version    string
	ChunkIndex uint
	Hash       string
	UpdatedAt  libtypes.Time
}

var columns = []string{
	"file_id",
	"chunk_id",
	"path",
	"f.size as file_size",
	"chunks.size as chunk_size",
	`chunk_index`,
	"hash",
	"f.updated_at",
}

/*
SELECT
  file_id,
  chunk_id,
  path,
  f.size as file_size,
  chunks.size as chunk_size
  repository_path,
  "chunk_index",
  hash,
  f.updated_at
FROM file_chunks fc
  INNER JOIN files f
    ON f.id = fc.file_id AND f.dataset_name = 'many' AND f.workspace = 'kuberlab-demo' AND version = '1.0.0'
  INNER JOIN chunks ON fc.chunk_id = chunks.id
*/
func (mgr *DatabaseMgr) GetFS(workspace, dataset, version string) (*io.ChunkedFileFS, error) {
	join1 := fmt.Sprintf(
		"INNER JOIN files f ON f.id = file_chunks.file_id AND f.dataset_name = '%v' AND version = '%v' AND f.workspace = '%v'",
		dataset, version, workspace,
	)
	rawFiles := make([]rawFile, 0)
	err := mgr.db.
		Table("file_chunks").
		Select(strings.Join(columns, ",")).
		Joins(join1).
		Joins("INNER JOIN chunks ON file_chunks.chunk_id = chunks.id").
		Order(`path, chunk_index`).
		Scan(&rawFiles).Error

	if err != nil {
		return nil, err
	}

	fs := &io.ChunkedFileFS{FS: make(map[string]*io.ChunkedFile)}
	for _, raw := range rawFiles {
		if f, ok := fs.FS[raw.Path]; ok {
			f.Chunks = append(f.Chunks, io.Chunk{Path: utils.GetHashedFilename(raw.Hash)})
			continue
		}
		fs.FS[raw.Path] = &io.ChunkedFile{
			Name:   "/" + raw.Path,
			Chunks: []io.Chunk{{Path: utils.GetHashedFilename(raw.Hash), Size: raw.ChunkSize}},
			Size:   raw.FileSize,
			Ref:    version,
			Fstat: &io.ChunkedFileInfo{
				Dir:      false,
				Fmode:    0644,
				FmodTime: raw.UpdatedAt.Time,
				Fname:    path.Base("/" + raw.Path),
				Fsize:    raw.FileSize,
			},
		}
	}
	//for _, file := range fileMap {
	//	fs.Files = append(fs.Files, file)
	//}
	return fs, nil
}
