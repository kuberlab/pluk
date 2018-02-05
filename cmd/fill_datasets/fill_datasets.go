package main

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/db"
	"log"
	"strings"
)

func main() {
	db.DbMgr = db.NewMainDatabaseMgr()

	reps := make([]*db.File, 0)

	mgr := db.DbMgr
	err := mgr.DB().Raw("SELECT DISTINCT repository_path from files").Scan(&reps).Error
	if err != nil {
		logrus.Error(err)
		return
	}

	for _, rep := range reps {
		splitted := strings.Split(rep.RepositoryPath, "/")
		name := splitted[len(splitted)-1]
		workspace := splitted[len(splitted)-2]
		_, err := mgr.GetDataset(workspace, name)
		if err != nil {
			// Create
			err = mgr.CreateDataset(&db.Dataset{Workspace: workspace, Name: name})
			if err != nil {
				log.Fatal(err)
			}
		}
		err = mgr.DB().Exec(fmt.Sprintf(
			"UPDATE files SET dataset_name='%v', workspace='%v' WHERE repository_path='%v' AND workspace IS NULL", name, workspace, rep.RepositoryPath),
		).Error
		if err != nil {
			log.Fatal(err)
		}
	}
}
