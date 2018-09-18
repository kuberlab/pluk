package main

import (
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/kuberlab/pluk/pkg/db"
	"github.com/kuberlab/pluk/pkg/gc"
)

func main() {
	db.DbMgr = db.NewMainDatabaseMgr()

	gc.ClearChunks(db.DbMgr)
}
