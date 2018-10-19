package db

import (
	"fmt"
	"path/filepath"
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
	GetFS(dsType, workspace, dataset, version string) (*io.ChunkedFileFS, error)
	DeleteRelatedFiles(dsType, workspace, dataset, version string) (int64, error)
	DeleteFiles(dsType, workspace, dataset, version, prefix string, preciseName bool) (int64, error)
	ListRelatedChunks(dsType, workspace, dataset, version string) ([]*FileChunk, error)
	ListRelatedChunksForFiles(dsType, workspace, dataset, version, prefix string, preciseName bool) ([]*FileChunk, error)
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

func (mgr *DatabaseMgr) ListRelatedChunksForFiles(
	dsType, workspace, dataset, version, prefix string, preciseName bool) ([]*FileChunk, error) {
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
		fmt.Sprintf("files.dataset_type='%v'", dsType),
		"file_chunks.file_id=files.id",
	}
	if version != "" {
		conditions = append(conditions, fmt.Sprintf("files.version='%v'", version))
	}
	if prefix != "" {
		var cond string
		if preciseName {
			cond = fmt.Sprintf("files.path = '%v'", prefix)
		} else {
			cond = fmt.Sprintf("files.path LIKE '%v%%'", prefix)
		}
		conditions = append(conditions, cond)
	}
	join := fmt.Sprintf("INNER JOIN files ON %v", strings.Join(conditions, " AND "))

	fileChunks := make([]*FileChunk, 0)
	err := mgr.db.
		Table("file_chunks").
		Select("chunk_id,file_id,chunk_index").
		Joins(join).
		Scan(&fileChunks).Error

	return fileChunks, err
}

func (mgr *DatabaseMgr) ListRelatedChunks(dsType, workspace, dataset, version string) ([]*FileChunk, error) {
	return mgr.ListRelatedChunksForFiles(dsType, workspace, dataset, version, "", false)
}

func (mgr *DatabaseMgr) DeleteFiles(dsType, workspace, dataset, version, prefix string, preciseName bool) (int64, error) {
	conditions := []string{
		fmt.Sprintf("files.workspace='%v'", workspace),
		fmt.Sprintf("files.dataset_name='%v'", dataset),
		fmt.Sprintf("files.dataset_type='%v'", dsType),
	}
	if version != "" {
		conditions = append(conditions, fmt.Sprintf("files.version='%v'", version))
	}
	if prefix != "" {
		var cond string
		if preciseName {
			cond = fmt.Sprintf("files.path = '%v'", prefix)
		} else {
			cond = fmt.Sprintf("files.path LIKE '%v%%'", prefix)
		}
		conditions = append(conditions, cond)
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

func (mgr *DatabaseMgr) DeleteRelatedFiles(dsType, workspace, dataset, version string) (int64, error) {
	return mgr.DeleteFiles(dsType, workspace, dataset, version, "", false)
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
  chunks.size as chunk_size,
  "chunk_index",
  hash,
  f.updated_at
FROM file_chunks fc
  INNER JOIN files f
    ON f.id = fc.file_id AND f.dataset_name = 'zappos' AND f.workspace = 'kuberlab-demo' AND version = '1.0.0'
  INNER JOIN chunks ON fc.chunk_id = chunks.id
*/
func (mgr *DatabaseMgr) GetFS(dsType, workspace, dataset, version string) (*io.ChunkedFileFS, error) {
	join1 := fmt.Sprintf(
		"INNER JOIN files f ON f.id = file_chunks.file_id "+
			"AND f.dataset_name = '%v' "+
			"AND version = '%v' "+
			"AND f.workspace = '%v' "+
			"AND f.dataset_type = '%v'",
		dataset, version, workspace, dsType,
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
	newFS := func(root string) *io.ChunkedFileFS {
		return &io.ChunkedFileFS{
			Files: make(map[string]*io.ChunkedFile),
			Root:  "/",
			Dirs:  make(map[string]*io.ChunkedFileFS),
		}
	}

	fs := newFS("/")
	for _, raw := range rawFiles {
		splitted := strings.Split(raw.Path, "/")
		curDir := fs
		for i, partPath := range splitted {
			if i == len(splitted)-1 {
				filePath := partPath
				if f, ok := curDir.Files[filePath]; ok {
					f.Chunks = append(f.Chunks, io.Chunk{Path: utils.GetHashedFilename(raw.Hash), Size: raw.ChunkSize})
					continue
				} else {
					curDir.Files[filePath] = &io.ChunkedFile{
						Name:   filePath,
						Chunks: []io.Chunk{{Path: utils.GetHashedFilename(raw.Hash), Size: raw.ChunkSize}},
						Size:   raw.FileSize,
						Ref:    version,
						Fstat: &io.ChunkedFileInfo{
							Dir:      false,
							Fmode:    0644,
							FmodTime: raw.UpdatedAt.Time,
							Fname:    partPath,
							Fsize:    raw.FileSize,
						},
					}
				}
			} else {
				dirname := "/" + strings.Join(splitted[:i+1], "/")
				if dirname != "/" {
					curDir.AddDir(dirname)
					curDir = curDir.Dirs[filepath.Base(dirname)]
				}
			}
		}
	}
	return fs, nil
}
