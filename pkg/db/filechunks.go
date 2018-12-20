package db

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

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
	GetRawFiles(dsType, workspace, dataset, version, prefix string, precise bool) ([]RawFile, error)
	DeleteRelatedFiles(dsType, workspace, dataset, version string) (int64, error)
	DeleteFiles(dsType, workspace, dataset, version, prefix string, preciseName bool) (int64, error)
	ListRelatedChunks(dsType, workspace, dataset, version string) ([]*FileChunk, error)
	ListRelatedChunksForFiles(dsType, workspace, dataset, version, prefix string, preciseName bool) ([]*FileChunk, error)
}

type FileChunk struct {
	FileID     uint `gorm:"unique_index:file_chunk_id" json:"file_id"`
	ChunkID    uint `gorm:"unique_index:file_chunk_id" json:"chunk_id"`
	ChunkIndex uint `json:"chunk_index"`
}

type FileChunkHash struct {
	FileChunk
	Chunk
}

func (mgr *DatabaseMgr) CreateFileChunk(file *FileChunk) error {
	if mgr.DBType() == "sqlite3" {
		tpl := "INSERT OR REPLACE INTO file_chunks " +
			"(file_id, chunk_id, chunk_index) VALUES (?, ?, ?)"
		return mgr.db.Exec(tpl, file.FileID, file.ChunkID, file.ChunkIndex).Error
	} else if mgr.DBType() == "postgres" {
		tpl := "INSERT INTO file_chunks " +
			"(file_id, chunk_id, chunk_index) VALUES (?, ?, ?) ON CONFLICT (file_id,chunk_id) DO NOTHING"
		return mgr.db.Exec(tpl, file.FileID, file.ChunkID, file.ChunkIndex).Error
	} else {
		return mgr.db.Create(file).Error
	}
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
		  AND files.dataset_type='dataset'
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

type RawFile struct {
	FileID     uint
	ChunkID    uint
	Path       string
	FileSize   int64
	FileMode   uint32
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
	"f.mode as file_mode",
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
  f.size      as file_size,
  f.mode      as file_mode,
  chunks.size as chunk_size,
  chunk_index,
  hash,
  f.updated_at
FROM "file_chunks"
  INNER JOIN files f
    ON f.id = file_chunks.file_id
       AND f.dataset_name = 'test'
       AND version = '1.0.0'
       AND f.workspace = 'kuberlab-demo'
       AND f.dataset_type = 'dataset'
  INNER JOIN chunks ON file_chunks.chunk_id = chunks.id
ORDER BY path, chunk_index;
*/
func (mgr *DatabaseMgr) GetRawFiles(dsType, workspace, dataset, version, prefix string, precise bool) ([]RawFile, error) {
	join1 := fmt.Sprintf(
		"INNER JOIN files f ON f.id = file_chunks.file_id " +
			"AND f.dataset_name = ? " +
			"AND f.workspace = ? " +
			"AND f.dataset_type = ?",
	)
	values := []interface{}{dataset, workspace, dsType}
	if version != "" {
		join1 = join1 + fmt.Sprintf(" AND version = ?")
		values = append(values, version)
	}
	if prefix != "" {
		if precise {
			join1 = join1 + fmt.Sprintf(" AND f.path = ?")
			values = append(values, prefix)
		} else {
			join1 = join1 + fmt.Sprintf(" AND f.path LIKE '%v%%'", prefix)
		}
	}
	rawFiles := make([]RawFile, 0)
	err := mgr.db.
		Table("file_chunks").
		Select(strings.Join(columns, ",")).
		Joins(join1, values...).
		Joins("INNER JOIN chunks ON file_chunks.chunk_id = chunks.id").
		Order(`path, chunk_index`).
		Scan(&rawFiles).Error
	return rawFiles, err
}

func (mgr *DatabaseMgr) GetFS(dsType, workspace, dataset, version string) (*io.ChunkedFileFS, error) {
	rawFiles, err := mgr.GetRawFiles(dsType, workspace, dataset, version, "", false)

	if err != nil {
		return nil, err
	}
	newFS := func(root string) *io.ChunkedFileFS {
		t := time.Now().Add(-time.Hour)
		if len(rawFiles) > 0 {
			t = rawFiles[0].UpdatedAt.Time
		}
		return &io.ChunkedFileFS{
			Files:   make(map[string]*io.ChunkedFile),
			Root:    "/",
			Dirs:    make(map[string]*io.ChunkedFileFS),
			ModTime: t,
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
							Fmode:    os.FileMode(raw.FileMode),
							FmodTime: raw.UpdatedAt.Time,
							Fname:    partPath,
							Fsize:    raw.FileSize,
						},
					}
				}
			} else {
				dirname := "/" + strings.Join(splitted[:i+1], "/")
				if dirname != "/" {
					curDir.AddDir(dirname, raw.UpdatedAt.Time)
					curDir = curDir.Dirs[filepath.Base(dirname)]
				}
			}
		}
	}
	return fs, nil
}
