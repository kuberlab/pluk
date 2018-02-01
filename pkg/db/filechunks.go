package db

type FileChunkMgr interface {
	CreateFileChunk(file *FileChunk) error
	GetFileChunk(fileID uint, chunkID uint) (*FileChunk, error)
	ListFileChunks(filter FileChunk) ([]*FileChunk, error)
}

type FileChunk struct {
	FileID  uint `gorm:"index:file_id"`
	ChunkID uint `gorm:"index:chunk_id"`
}

func (mgr *DatabaseMgr) CreateFileChunk(file *FileChunk) error {
	return mgr.db.Create(file).Error
}

func (mgr *DatabaseMgr) GetFileChunk(fileID uint, chunkID uint) (*FileChunk, error) {
	var fileChunk = FileChunk{}
	err := mgr.db.First(&fileChunk, FileChunk{FileID: fileID, ChunkID: chunkID}).Error
	return &fileChunk, err
}

func (mgr *DatabaseMgr) ListFileChunks(filter FileChunk) ([]*FileChunk, error) {
	var fileChunks = make([]*FileChunk, 0)
	err := mgr.db.Find(&fileChunks, filter).Error
	return fileChunks, err
}
