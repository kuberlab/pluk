package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/kuberlab/pacak/pkg/pacakimpl"
	datasets2 "github.com/kuberlab/pluk/pkg/datasets"
	"github.com/kuberlab/pluk/pkg/db"
	"github.com/kuberlab/pluk/pkg/utils"
)

func main() {
	gitLocalDir := utils.GitLocalDir()
	wsNames, err := populateDirs(gitLocalDir)
	if err != nil {
		log.Fatal(err)
	}

	db.DbMgr = db.NewMainDatabaseMgr()
	gitIface := pacakimpl.NewGitInterface(utils.GitDir(), utils.GitLocalDir())
	dsManager := datasets2.NewManager(gitIface)

	datasets := make([]*datasets2.Dataset, 0)
	for _, wsName := range wsNames {
		datasetDirs, err := populateDirs(wsName)
		if err != nil {
			log.Fatal(err)
		}
		for _, ds := range datasetDirs {
			dataset := dsManager.NewDataset(
				strings.Split(wsName, "/")[2],
				strings.Split(ds, "/")[3],
			)
			dataset.InitRepo(true)
			datasets = append(datasets, dataset)
		}
	}

	for _, ds := range datasets {
		versions, err := ds.Versions()
		if err != nil {
			log.Fatal(err)
		}
		for _, v := range versions {
			fmt.Println(ds.Name, v)

			fs, err := ds.GetFSStructureFromRepo(v)
			if err != nil {
				log.Fatal(err)
			}
			if err = ds.SaveFSLocally(fs, v); err != nil {
				log.Fatal(err)
			}
		}
	}
	//mgr := db.NewMainDatabaseMgr()
	//for i, ds := range datasetPaths {
	//	repo := strings.TrimPrefix(ds, utils.GitLocalDir()+"/")
	//	pacakRepo, err := gitIface.GetRepository(repo)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	tags, err := pacakRepo.TagList()
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	for _, tag := range tags {
	//		if err = pacakRepo.Checkout(tag); err != nil {
	//			log.Fatal(err)
	//		}
	//		fileCount := 0
	//		err = filepath.Walk(ds, func(path string, info os.FileInfo, err error) error {
	//			if info.IsDir() {
	//				return nil
	//			}
	//			relativePath := strings.TrimPrefix(path, ds)
	//			if strings.HasPrefix(relativePath, "/.") {
	//				return nil
	//			}
	//			fileCount++
	//			chunked, err := plukio.NewInternalChunked(pacakRepo, tag, relativePath)
	//			if err != nil {
	//				log.Fatal(err)
	//			}
	//			chunked = chunked
	//			return nil
	//		})
	//		fmt.Printf("REPO=%v\tFILES=%v", ds, fileCount)
	//		if err != nil {
	//			log.Fatal(err)
	//		}
	//	}
	//	if i == 2 {
	//		log.Fatal("ololo")
	//	}
	//}
}

func populateDirs(root string) ([]string, error) {
	dir, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	subdirs, err := dir.Readdirnames(0)
	if err != nil {
		if err == io.EOF {
			return []string{}, nil
		}
		return nil, err
	}
	var subdirPaths []string
	for _, path := range subdirs {
		subdirPaths = append(subdirPaths, root+"/"+path)
	}
	return subdirPaths, nil
}
