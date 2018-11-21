package db

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/jinzhu/gorm"
	"github.com/kuberlab/pluk/pkg/config"
	"github.com/kuberlab/pluk/pkg/utils"
)

var mainDB *gorm.DB

type postCreateFunc func(*gorm.DB) error

func InitFake(postCreate postCreateFunc, fname string) *gorm.DB {
	db, err := gorm.Open("sqlite3", fname)
	if err != nil {
		logrus.Panic("Can't create sqlite database: ", err)
	}
	_, err = db.DB().Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		logrus.Panic(err)
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
	dbType, connString := config.GetConnString()

	if dbType == "sqlite3" {
		logrus.Infof("Opening sqlite DB at %v...", connString)
	} else {
		logrus.Infof(
			"Opening %v DB [dbname=%v, host=%v]...",
			dbType, utils.DBName(), utils.DBHost(),
		)
	}

	db, err := gorm.Open(dbType, connString)
	if err != nil {
		if dbType == "postgres" {
			_, conn := config.GetConnStringForName("postgres")
			db, errC := gorm.Open(dbType, conn)
			if errC != nil {
				logrus.Error(errC)
			}
			db = db.LogMode(true)
			db.SetLogger(gorm.Logger{mainDBLogger{}})
			logrus.Infof("Trying to create database %v...", utils.DBName())
			// Try create database
			_, errC = db.DB().Exec(fmt.Sprintf("CREATE DATABASE %q", utils.DBName()))
			if errC == nil {
				err = nil
				db.Close()
				db, err = gorm.Open(dbType, connString)
			} else {
				logrus.Error(errC)
			}
		}
		if err != nil {
			logrus.Panic("Can't open database: ", err)
		}
	}

	if dbType == "sqlite3" {
		// Enable WAL mode for sqlite3
		_, err = db.DB().Exec("PRAGMA journal_mode=WAL")
		if err != nil {
			logrus.Panic(err)
		}
	}
	db.DB().SetMaxIdleConns(10)
	db.DB().SetMaxOpenConns(100)
	if err != nil {
		logrus.Panic("Error configure database: ", err)
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
