package main

import (
	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/api"
	"github.com/kuberlab/pluk/pkg/utils"
	"github.com/kuberlab/pluk/pkg/db"
)

func main() {
	if utils.DebugEnabled() {
		logrus.SetLevel(logrus.DebugLevel)
	}
	logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, TimestampFormat: "2006-01-02 15:04:05"})
	db.DbMgr = db.NewMainDatabaseMgr()
	api.Start()
}
