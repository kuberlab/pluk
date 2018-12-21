package db

import (
	"bytes"
	"fmt"
	"strings"
)

type ChunkMgr interface {
	CreateChunk(chunk *Chunk) error
	CreateChunks(raws []RawFile) ([]*Chunk, error)
	UpdateChunk(chunk *Chunk) (*Chunk, error)
	GetChunk(hash string) (*Chunk, error)
	GetChunkByID(chunkID uint) (*Chunk, error)
	ListChunks(filter Chunk) ([]*Chunk, error)
	DeleteChunk(id uint) error
	DeleteChunks(chunks []Chunk) error
}

type Chunk struct {
	BaseModel
	ID   uint   `sql:"AUTO_INCREMENT"`
	Hash string `json:"hash" gorm:"primary_key"`
	Size int64  `json:"size"`
}

func (mgr *DatabaseMgr) CreateChunk(chunk *Chunk) error {
	if mgr.DBType() == "postgres" {
		tpl := "INSERT INTO chunks " +
			"(hash, size) VALUES (?, ?) ON CONFLICT (hash) DO UPDATE SET size=? RETURNING id"
		var newC = &Chunk{}
		err := mgr.db.Raw(tpl, chunk.Hash, chunk.Size, chunk.Size).Scan(newC).Error
		if err != nil {
			return err
		}
		chunk.ID = newC.ID
		return nil

	} else if mgr.DBType() == "sqlite3" {
		tpl := "INSERT INTO chunks " +
			"(hash, size) VALUES (?, ?) ON CONFLICT (hash) DO UPDATE SET size=?"
		err := mgr.db.Exec(tpl, chunk.Hash, chunk.Size, chunk.Size).Error
		if err != nil {
			return err
		}
		updated, err := mgr.GetChunk(chunk.Hash)
		if err != nil {
			return err
		}
		chunk.ID = updated.ID
		return nil
	} else {
		return mgr.db.Create(chunk).Error
	}
}

func (mgr *DatabaseMgr) ListChunksByHash(hashes []RawFile) ([]*Chunk, error) {
	where := bytes.NewBufferString("hash IN (")
	ids := make([]string, 0)
	for _, h := range hashes {
		ids = append(ids, fmt.Sprintf("'%v'", h.Hash))
	}
	where.WriteString(strings.Join(ids, ","))
	where.WriteString(")")
	chunks := make([]*Chunk, 0)
	err := mgr.db.
		Where(where.String()).
		Find(&chunks).Error

	hashMap := make(map[string]int)
	for i, hash := range hashes {
		hashMap[hash.Hash] = i
	}

	newChunks := make([]*Chunk, len(chunks))
	for _, c := range chunks {
		place := hashMap[c.Hash]
		newChunks[place] = c
	}

	return newChunks, err
}

func (mgr *DatabaseMgr) CreateChunks(raws []RawFile) ([]*Chunk, error) {
	sql := bytes.NewBufferString("")
	var chunks = make([]*Chunk, 0)
	if mgr.DBType() == "postgres" {
		sql.WriteString("INSERT INTO chunks (hash, size) VALUES ")
		values := make([]string, 0)
		for _, raw := range raws {
			values = append(values, fmt.Sprintf(`('%v', %v)`, raw.Hash, raw.ChunkSize))
		}
		sql.WriteString(strings.Join(values, ","))
		sql.WriteString(" ON CONFLICT (hash) DO UPDATE SET size=excluded.size")
		err := mgr.db.Exec(sql.String()).Error
		if err != nil {
			return nil, err
		}
		return mgr.ListChunksByHash(raws)
	} else if mgr.DBType() == "sqlite3" {
		sql.WriteString("INSERT INTO chunks (hash, size) VALUES ")
		values := make([]string, 0)
		for _, raw := range raws {
			values = append(values, fmt.Sprintf(`('%v', %v)`, raw.Hash, raw.ChunkSize))
		}
		sql.WriteString(strings.Join(values, ","))
		sql.WriteString(" ON CONFLICT (hash) DO UPDATE SET size=excluded.size")

		err := mgr.db.Exec(sql.String()).Error
		if err != nil {
			return nil, err
		}

		return mgr.ListChunksByHash(raws)
	} else {
		for _, raw := range raws {
			chunk := &Chunk{Hash: raw.Hash, Size: raw.ChunkSize}
			err := mgr.CreateChunk(chunk)
			if err != nil {
				return nil, err
			}
			chunks = append(chunks, chunk)
		}
		return chunks, nil
	}
}

func (mgr *DatabaseMgr) UpdateChunk(chunk *Chunk) (*Chunk, error) {
	err := mgr.db.Save(chunk).Error
	return chunk, err
}

func (mgr *DatabaseMgr) GetChunk(hash string) (*Chunk, error) {
	var chunk = Chunk{}
	err := mgr.db.First(&chunk, Chunk{Hash: hash}).Error
	return &chunk, err
}

func (mgr *DatabaseMgr) GetChunkByID(chunkID uint) (*Chunk, error) {
	var chunk = Chunk{}
	err := mgr.db.First(&chunk, Chunk{ID: chunkID}).Error
	return &chunk, err
}

func (mgr *DatabaseMgr) ListChunks(filter Chunk) ([]*Chunk, error) {
	var chunks = make([]*Chunk, 0)
	err := mgr.db.Find(&chunks, filter).Error
	return chunks, err
}

func (mgr *DatabaseMgr) DeleteChunk(id uint) error {
	return mgr.db.Delete(Chunk{}, Chunk{ID: id}).Error
}

func (mgr *DatabaseMgr) DeleteChunks(chunks []Chunk) error {
	where := bytes.NewBufferString("id IN (")
	ids := make([]string, 0)
	for _, c := range chunks {
		ids = append(ids, fmt.Sprintf("%v", c.ID))
	}
	where.WriteString(strings.Join(ids, ","))
	where.WriteString(")")
	err := mgr.db.Where(where.String()).Delete(&chunks).Error
	return err
}
