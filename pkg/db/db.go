package db

import (
	"os"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/kuberlab/pluk/pkg/db/gorm"
	"github.com/kuberlab/pluk/pkg/utils"
)

var DbMgr DataMgr

type DataMgr interface {
	// All models DB interfaces here.
	ChunkMgr
	FileMgr
	FileChunkMgr
	DatasetMgr
	DatasetVersionVersionMgr
	DB() *gorm.DB
	DBType() string
	Begin() *DatabaseMgr
	Commit() *DatabaseMgr
	Rollback() *DatabaseMgr
	Close() error
}
type DatabaseMgr struct {
	db     *gorm.DB
	dbType string
}

func (mgr *DatabaseMgr) Close() error {
	return mgr.db.Close()
}
func NewDatabaseMgr(db *gorm.DB) *DatabaseMgr {
	return &DatabaseMgr{
		db:     db,
		dbType: utils.DBType(),
	}
}

func NewMainDatabaseMgr() *DatabaseMgr {
	return NewDatabaseMgr(db.InitMain(CreateAll))
}
func NewFakeDatabaseMgr(fname string) *DatabaseMgr {
	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		os.Setenv("DB_NAME", fname)
	}
	return NewDatabaseMgr(db.InitMain(CreateAll))
}

func (mgr *DatabaseMgr) DB() *gorm.DB {
	return mgr.db
}

func (mgr *DatabaseMgr) DBType() string {
	return mgr.dbType
}

func (mgr *DatabaseMgr) Begin() *DatabaseMgr {
	return &DatabaseMgr{
		db:     mgr.db.Begin(),
		dbType: mgr.dbType,
	}
}

func (mgr *DatabaseMgr) Commit() *DatabaseMgr {
	return &DatabaseMgr{
		db:     mgr.db.Commit(),
		dbType: mgr.dbType,
	}
}

func (mgr *DatabaseMgr) Rollback() *DatabaseMgr {
	return &DatabaseMgr{
		db:     mgr.db.Rollback(),
		dbType: mgr.dbType,
	}
}
