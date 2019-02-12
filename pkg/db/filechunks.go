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
	CreateFileChunks(fileChunks []*FileChunk) error
	GetFileChunk(fileID uint, chunkID uint, index int) (*FileChunk, error)
	ListFileChunks(filter FileChunk) ([]*FileChunk, error)
	DeleteFileChunk(fileID, chunkID uint) error
	GetFS(dsType, workspace, dataset, version string) (*io.ChunkedFileFS, error)
	GetRawFiles(dsType, workspace, dataset, version, prefix string, precise bool) ([]RawFile, error)
	DeleteRelatedFiles(dsType, workspace, dataset, version string) (int64, error)
	DeleteFiles(dsType, workspace, dataset, version, prefix string, preciseName bool) (int64, error)
	ListRelatedChunks(dsType, workspace, dataset, version string) ([]*FileChunk, error)
	ListRelatedChunksForFiles(dsType, workspace, dataset, version, prefix string, preciseName bool) ([]*FileChunk, error)
	ListFileChunksByChunks(chunks []Chunk) ([]*FileChunk, error)
}

type FileChunk struct {
	FileID     uint `gorm:"unique_index:file_chunk_id_index" json:"file_id"`
	ChunkID    uint `gorm:"unique_index:file_chunk_id_index" json:"chunk_id"`
	ChunkIndex uint `gorm:"unique_index:file_chunk_id_index" json:"chunk_index"`
}

type FileChunkHash struct {
	FileChunk
	Chunk
}

func (mgr *DatabaseMgr) CreateFileChunk(file *FileChunk) error {
	if mgr.DBType() == "sqlite3" {
		tpl := "INSERT INTO file_chunks " +
			"(file_id, chunk_id, chunk_index) VALUES (?, ?, ?) ON CONFLICT (file_id,chunk_id) DO NOTHING"
		return mgr.db.Exec(tpl, file.FileID, file.ChunkID, file.ChunkIndex).Error
	} else if mgr.DBType() == "postgres" {
		tpl := "INSERT INTO file_chunks " +
			"(file_id, chunk_id, chunk_index) VALUES (?, ?, ?) ON CONFLICT (file_id,chunk_id) DO NOTHING"
		return mgr.db.Exec(tpl, file.FileID, file.ChunkID, file.ChunkIndex).Error
	} else {
		return mgr.db.Create(file).Error
	}
}

func (mgr *DatabaseMgr) CreateFileChunks(fileChunks []*FileChunk) error {
	sql := strings.Builder{}
	if mgr.DBType() == "postgres" {
		sql.WriteString("INSERT INTO file_chunks (file_id, chunk_id, chunk_index) VALUES ")
		values := make([]string, 0)
		for _, raw := range fileChunks {
			values = append(values, fmt.Sprintf(`(%v,%v,%v)`, raw.FileID, raw.ChunkID, raw.ChunkIndex))
		}
		sql.WriteString(strings.Join(values, ","))
		sql.WriteString(" ON CONFLICT (file_id, chunk_id, chunk_index) DO NOTHING")
		return mgr.db.Exec(sql.String()).Error
	} else if mgr.DBType() == "sqlite3" {
		// Get next insert ID
		sql.WriteString("INSERT INTO file_chunks (file_id, chunk_id, chunk_index) VALUES ")
		values := make([]string, 0)
		for _, raw := range fileChunks {
			values = append(values, fmt.Sprintf(`(%v, %v, %v)`, raw.FileID, raw.ChunkID, raw.ChunkIndex))
		}
		sql.WriteString(strings.Join(values, ","))
		sql.WriteString(" ON CONFLICT (file_id, chunk_id, chunk_index) DO NOTHING")

		return mgr.db.Exec(sql.String()).Error
	} else {
		for _, raw := range fileChunks {
			fileChunk := &FileChunk{FileID: raw.FileID, ChunkID: raw.ChunkID}
			err := mgr.db.Create(fileChunk).Error
			if err != nil {
				return err
			}
		}
		return nil
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

func (mgr *DatabaseMgr) ListFileChunksByChunks(chunks []Chunk) ([]*FileChunk, error) {
	var fileChunks = make([]*FileChunk, 0)
	where := strings.Builder{}
	where.WriteString("chunk_id IN (")

	ids := make([]string, 0)
	for _, c := range chunks {
		ids = append(ids, fmt.Sprintf("%v", c.ID))
	}
	where.WriteString(strings.Join(ids, ","))
	where.WriteString(")")
	err := mgr.db.Where(where.String()).Find(&fileChunks).Error
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
	values := []interface{}{workspace, dataset, dsType}
	conditions := []string{
		"files.workspace=?",
		"files.dataset_name=?",
		"files.dataset_type=?",
		"file_chunks.file_id=files.id",
	}
	if version != "" {
		conditions = append(conditions, "files.version=?")
		values = append(values, version)
	}
	if prefix != "" {
		var cond string
		prefix = strings.Replace(prefix, "'", "''", -1)
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
		Joins(join, values...).
		Scan(&fileChunks).Error

	return fileChunks, err
}

func (mgr *DatabaseMgr) ListRelatedChunks(dsType, workspace, dataset, version string) ([]*FileChunk, error) {
	return mgr.ListRelatedChunksForFiles(dsType, workspace, dataset, version, "", false)
}

func (mgr *DatabaseMgr) DeleteFiles(dsType, workspace, dataset, version, prefix string, preciseName bool) (int64, error) {
	values := []interface{}{workspace, dataset, dsType}
	conditions := []string{
		"files.workspace=?",
		"files.dataset_name=?",
		"files.dataset_type=?",
	}
	if version != "" {
		conditions = append(conditions, "files.version=?")
		values = append(values, version)
	}
	if prefix != "" {
		var cond string
		prefix = strings.Replace(prefix, "'", "''", -1)
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

	err := mgr.db.Exec(sqlDeleteRelation, values...).Error
	if err != nil {
		return 0, err
	}

	sqlDeleteFiles := fmt.Sprintf("DELETE FROM files where %v", condition)
	db := mgr.db.Exec(sqlDeleteFiles, values...)

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
	join := strings.Builder{}
	join.WriteString(
		"INNER JOIN files f ON f.id = file_chunks.file_id " +
			"AND f.dataset_name = ? " +
			"AND f.workspace = ? " +
			"AND f.dataset_type = ?",
	)
	values := []interface{}{dataset, workspace, dsType}
	if version != "" {
		join.WriteString(" AND version = ?")
		values = append(values, version)
	}
	if prefix != "" {
		if precise {
			join.WriteString(" AND f.path = ?")
			values = append(values, prefix)
		} else {
			prefix = strings.Replace(prefix, "'", "''", -1)
			join.WriteString(fmt.Sprintf(" AND f.path LIKE '%v%%'", prefix))
		}
	}
	rawFiles := make([]RawFile, 0)
	err := mgr.db.
		Table("file_chunks").
		Select(strings.Join(columns, ",")).
		Joins(join.String(), values...).
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
				if f, ok := curDir.Files[partPath]; ok {
					f.Chunks = append(f.Chunks, io.Chunk{Path: utils.GetHashedFilename(raw.Hash), Size: raw.ChunkSize})
					continue
				} else {
					curDir.Files[partPath] = &io.ChunkedFile{
						Name:    partPath,
						Chunks:  []io.Chunk{{Path: utils.GetHashedFilename(raw.Hash), Size: raw.ChunkSize}},
						Size:    raw.FileSize,
						Dir:     false,
						Mode:    os.FileMode(raw.FileMode),
						ModTime: raw.UpdatedAt.Time,
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
