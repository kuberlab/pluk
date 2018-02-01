package db

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/jinzhu/gorm"
	"github.com/kuberlab/pluk/pkg/utils"
)

var mainDB *gorm.DB

type postCreateFunc func(*gorm.DB) error

func InitFake(postCreate postCreateFunc) *gorm.DB {
	db, err := gorm.Open("sqlite3", fmt.Sprintf("%v/pluke.db", utils.DataDir()))
	if err != nil {
		logrus.Panic("Can't create sqlite database: ", err)
	}
	db.DB().SetMaxIdleConns(10)
	db.DB().SetMaxOpenConns(100)
	if err != nil {
		logrus.Panic("Error configure sqlite database: ", err)
	}
	db = db.LogMode(false)
	db.SetLogger(gorm.Logger{mainDBLogger{}})
	mainDB = db

	if err := postCreate(mainDB); err != nil {
		panic(err)
	}
	return mainDB
}

func InitMain(postCreate postCreateFunc) *gorm.DB {
	db, err := gorm.Open("sqlite3", fmt.Sprintf("/pluk/pluke.db"))
	if err != nil {
		logrus.Panic("Can't create sqlite database: ", err)
	}
	db.DB().SetMaxIdleConns(10)
	db.DB().SetMaxOpenConns(100)
	if err != nil {
		logrus.Panic("Error configure sqlite database: ", err)
	}
	db = db.LogMode(utils.DebugEnabled())
	db.SetLogger(gorm.Logger{mainDBLogger{}})
	mainDB = db

	if err := postCreate(mainDB); err != nil {
		panic(err)
	}
	return mainDB
}

type mainDBLogger struct {
}

func (l mainDBLogger) Println(v ...interface{}) {
	logrus.Infoln(v)
}
