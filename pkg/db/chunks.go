package db

type ChunkMgr interface {
	CreateChunk(chunk *Chunk) error
	UpdateChunk(chunk *Chunk) (*Chunk, error)
	GetChunk(chunkID uint) (*Chunk, error)
	ListChunks(filter Chunk) ([]*Chunk, error)
}

type Chunk struct {
	BaseModel
	ID    uint   `sql:"AUTO_INCREMENT" gorm:"primary_key"`
	Index uint   `json:"index"`
	Hash  string `json:"hash" gorm:"index:idx_hash"`
	Files []File `gorm:"many2many:file_chunks;"`
}

func (mgr *DatabaseMgr) CreateChunk(chunk *Chunk) error {
	return mgr.db.Create(chunk).Error
}

func (mgr *DatabaseMgr) UpdateChunk(chunk *Chunk) (*Chunk, error) {
	if _, err := mgr.GetChunk(chunk.ID); err != nil {
		return nil, err
	}
	err := mgr.db.Save(chunk).Error
	return chunk, err
}

func (mgr *DatabaseMgr) GetChunk(chunkID uint) (*Chunk, error) {
	var chunk = Chunk{}
	err := mgr.db.First(&chunk, Chunk{ID: chunkID}).Error
	return &chunk, err
}

func (mgr *DatabaseMgr) ListChunks(filter Chunk) ([]*Chunk, error) {
	var chunks = make([]*Chunk, 0)
	err := mgr.db.Find(&chunks, filter).Error
	return chunks, err
}
