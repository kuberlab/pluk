package main

import (
	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/api"
	"github.com/kuberlab/pluk/pkg/db"
	"github.com/kuberlab/pluk/pkg/gc"
	"github.com/kuberlab/pluk/pkg/grpc"
	"github.com/kuberlab/pluk/pkg/utils"
)

func main() {
	if utils.DebugEnabled() {
		logrus.SetLevel(logrus.DebugLevel)
	}
	logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, TimestampFormat: "2006-01-02 15:04:05"})
	db.DbMgr = db.NewMainDatabaseMgr()
	go gc.Start()
	go grpc.Start()
	api.Start()
}
