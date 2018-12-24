package db

import (
	"bytes"
	"fmt"
	"strings"
)

type ChunkMgr interface {
	CreateChunk(chunk *Chunk) error
	CreateChunks(raws []*RawFile) error
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

func (mgr *DatabaseMgr) ListChunksByHash(hashes []*RawFile) ([]*Chunk, error) {
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

// Inserts Chunk IDs in place
func (mgr *DatabaseMgr) CreateChunks(raws []*RawFile) error {
	sql := bytes.NewBufferString("")
	var chunks = make([]*Chunk, 0)

	// What if there are duplicates of chunk hashes somewhere in raws?
	// Need to:
	// 1. Delete duplicates
	// 2. Send them to DB
	// 3. Get non-duplicated chunks
	exclusivesMap := make(map[string]*RawFile)
	chunkMap := make(map[string][]*RawFile)
	for _, raw := range raws {
		exclusivesMap[raw.Hash] = raw
		if _, ok := chunkMap[raw.Hash]; ok {
			chunkMap[raw.Hash] = append(chunkMap[raw.Hash], raw)
		} else {
			chunkMap[raw.Hash] = []*RawFile{raw}
		}
	}
	exclusives := make([]*RawFile, len(exclusivesMap))
	i := 0
	for _, ex := range exclusivesMap {
		exclusives[i] = ex
		i++
	}

	if mgr.DBType() == "postgres" {
		sql.WriteString("INSERT INTO chunks (hash, size) VALUES ")
		values := make([]string, 0)
		for _, raw := range exclusives {
			values = append(values, fmt.Sprintf(`('%v', %v)`, raw.Hash, raw.ChunkSize))
		}
		sql.WriteString(strings.Join(values, ","))
		sql.WriteString(" ON CONFLICT (hash) DO UPDATE SET size=excluded.size")
		err := mgr.db.Exec(sql.String()).Error
		if err != nil {
			return err
		}
		chunks, err = mgr.ListChunksByHash(exclusives)
		if err != nil {
			return err
		}
	} else if mgr.DBType() == "sqlite3" {
		sql.WriteString("INSERT INTO chunks (hash, size) VALUES ")
		values := make([]string, 0)
		for _, raw := range exclusives {
			values = append(values, fmt.Sprintf(`('%v', %v)`, raw.Hash, raw.ChunkSize))
		}
		sql.WriteString(strings.Join(values, ","))
		sql.WriteString(" ON CONFLICT (hash) DO UPDATE SET size=excluded.size")

		err := mgr.db.Exec(sql.String()).Error
		if err != nil {
			return err
		}

		chunks, err = mgr.ListChunksByHash(exclusives)
		if err != nil {
			return err
		}
	} else {
		for _, raw := range exclusives {
			chunk := &Chunk{Hash: raw.Hash, Size: raw.ChunkSize}
			err := mgr.CreateChunk(chunk)
			if err != nil {
				return err
			}
			chunks = append(chunks, chunk)
		}
	}

	// Got exclusive chunks.
	// Need to distribute them to appropriate files.
	for _, chunk := range chunks {
		raws := chunkMap[chunk.Hash]
		for _, raw := range raws {
			raw.ChunkID = chunk.ID
		}
	}
	return nil
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
