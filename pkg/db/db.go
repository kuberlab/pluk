package db

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/kuberlab/pluk/pkg/db/gorm"
)

var DbMgr DataMgr

type DataMgr interface {
	// All models DB interfaces here.
	ChunkMgr
	FileMgr
	Begin() *DatabaseMgr
	Commit() *DatabaseMgr
	Rollback() *DatabaseMgr
	Close() error
}
type DatabaseMgr struct {
	db *gorm.DB
}

func (mgr *DatabaseMgr) Close() error {
	return mgr.db.Close()
}
func NewDatabaseMgr(db *gorm.DB) *DatabaseMgr {
	return &DatabaseMgr{
		db: db,
	}
}

func NewMainDatabaseMgr() *DatabaseMgr {
	return NewDatabaseMgr(db.InitMain(CreateAll))
}
func NewFakeDatabaseMgr() *DatabaseMgr {
	return NewDatabaseMgr(db.InitFake(CreateTables))
}

func (mgr *DatabaseMgr) Begin() *DatabaseMgr {
	return &DatabaseMgr{
		db: mgr.db.Begin(),
	}
}

func (mgr *DatabaseMgr) Commit() *DatabaseMgr {
	return &DatabaseMgr{
		db: mgr.db.Commit(),
	}
}

func (mgr *DatabaseMgr) Rollback() *DatabaseMgr {
	return &DatabaseMgr{
		db: mgr.db.Rollback(),
	}
}
