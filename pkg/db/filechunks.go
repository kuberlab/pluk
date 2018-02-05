package db

type FileChunkMgr interface {
	CreateFileChunk(file *FileChunk) error
	GetFileChunk(fileID uint, chunkID uint, index int) (*FileChunk, error)
	ListFileChunks(filter FileChunk) ([]*FileChunk, error)
	DeleteFileChunk(fileID, chunkID uint) error
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

/*
SELECT
  file_id,
  chunk_id,
  path,
  "size",
  repository_path,
  "index",
  hash
FROM file_chunks fc
  INNER JOIN files f
    ON f.id = fc.file_id AND repository_path = '/git-local/kuberlab-demo/many' AND version = '1.0.0'
  INNER JOIN chunks ON fc.chunk_id = chunks.id
*/
func (mgr *DatabaseMgr) GetFS(repo, version string) {

}
