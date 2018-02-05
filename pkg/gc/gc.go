package gc

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/datasets"
	"github.com/kuberlab/pluk/pkg/db"
	"github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

const (
	gcInterval = time.Hour
)

func Start() {
	goGC()

	ticker := time.NewTicker(gcInterval)
	for {
		select {
		case <-ticker.C:
			goGC()
		}
	}
}

func goGC() {
	logrus.Info("Starting garbage collector...")
	mgr := db.DbMgr

	vDatasets, err := mgr.ListDatasets(db.Dataset{Deleted: true})
	if err != nil {
		logrus.Error(err)
		return
	}

	// TODO: sqlite allows only 1 transaction at a time.
	// TODO: So, if we create transaction here, all another requests
	// TODO: (like list dataset or versions) will hang until transaction is completed
	tx := mgr.Begin()
	var needCloseTx = true
	endTx := func() {
		if !needCloseTx {
			return
		}
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
		needCloseTx = false
	}
	defer endTx()

	deleted := 0
	var files []*db.File

	// First: check if repo exists.
	for _, ds := range vDatasets {
		// Delete all files within this repo
		files, err = tx.ListFiles(db.File{Workspace: ds.Workspace, DatasetName: ds.Name})
		if err != nil {
			logrus.Error(err)
			return
		}

		for _, f := range files {
			if checkAndDeleteFile(tx, f) {
				deleted++
			}
			if deleted%100 == 0 {
				logrus.Infof("Deleted %v objects", deleted)
			}
		}
		deleteDataset(tx, ds)
	}

	// Second: Iterate over versions and see if the corresponding version deleted.
	// TODO: ^^
	endTx()

	// Third: See if there deleted dataset on master; delete those which don't exist on master
	// but exist on slave.
	if utils.HasMasters() {
		// Sync with master and delete obsolete datasets.
		gcFromMasters(mgr)
	}
	logrus.Infof("Done garbage collecting. Deleted %v objects.", deleted)
}

func deleteDataset(mgr db.DataMgr, d *db.Dataset) {
	sql := fmt.Sprintf("DELETE FROM dataset_versions WHERE workspace='%v' AND name='%v'", d.Workspace, d.Name)
	err := mgr.DB().Exec(sql).Error
	if err != nil {
		logrus.Error(err)
		return
	}
	mgr.DeleteDataset(d.ID)
}

func checkAndDeleteFile(mgr db.DataMgr, f *db.File) bool {
	mgr.DeleteFile(f.ID)

	chunkIDs, err := mgr.ListFileChunks(db.FileChunk{FileID: f.ID})
	if err != nil {
		logrus.Error(err)
		return false
	}
	for _, chunkID := range chunkIDs {
		chunk, err := mgr.GetChunkByID(chunkID.ChunkID)
		if err != nil {
			logrus.Error(err)
			return false
		}
		checkAndDeleteChunk(mgr, chunk, f)
	}
	return true
}

func checkAndDeleteChunk(mgr db.DataMgr, chunk *db.Chunk, f *db.File) {
	mgr.DeleteFileChunk(f.ID, chunk.ID)

	// See if there are more connections on this chunk
	remain, err := mgr.ListFileChunks(db.FileChunk{ChunkID: chunk.ID})
	if err != nil {
		logrus.Error(err)
		return
	}
	// If there are no connections, completely delete this chunk.
	if len(remain) == 0 {
		path := utils.GetHashedFilename(chunk.Hash)
		mgr.DeleteChunk(chunk.ID)
		os.Remove(path)

		dirName := filepath.Dir(path)
		remainFiles, err := ioutil.ReadDir(dirName)
		if err != nil {
			logrus.Error(err)

		}

		// If there are no files in this directory, delete it.
		if len(remainFiles) == 0 {
			os.RemoveAll(dirName)
		}
	}
}

func gcFromMasters(mgr db.DataMgr) {
	//gitIface := pacakimpl.NewGitInterface(utils.GitDir(), utils.GitLocalDir())
	var needCloseTx = true
	var err error
	tx := mgr.Begin()
	endTx := func() {
		if !needCloseTx {
			return
		}
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
		needCloseTx = false
	}
	defer endTx()

	dsManager := datasets.NewManager(tx)
	vDatasets, err := tx.ListDatasets(db.Dataset{})
	if err != nil {
		logrus.Error(err)
		return
	}

	// Get list of available slaveDatasets: map[workspace][]dataset_name
	localDatasets := make(map[string]map[string]bool, 0)
	for _, ds := range vDatasets {
		if ds.Deleted {
			continue
		}
		if _, ok := localDatasets[ds.Workspace]; ok {
			localDatasets[ds.Workspace][ds.Name] = true
		} else {
			localDatasets[ds.Workspace] = map[string]bool{ds.Name: true}
		}
	}

	candidates := make([]types.Dataset, 0)
	for ws, slaveDatasets := range localDatasets {
		remoteDatasets, err := io.MasterClient.ListDatasets(ws)
		if err != nil {
			logrus.Error(err)
			return
		}
		masterDatasetMap := make(map[string]bool)
		for _, ds := range remoteDatasets.Datasets {
			masterDatasetMap[ds.Name] = true
		}
		// Check if master has specific dataset on slave.
		// If it doesn't exists, then it was probably deleted.
		// Then we delete it as well on the slave.
		for slaveName := range slaveDatasets {
			if _, ok := masterDatasetMap[slaveName]; !ok {
				candidates = append(candidates, types.Dataset{Name: slaveName, Workspace: ws})
			}
		}
	}

	for _, candidate := range candidates {
		logrus.Infof("Delete dataset %v/%v from slave", candidate.Workspace, candidate.Name)
		if err = dsManager.DeleteDataset(candidate.Workspace, candidate.Name); err != nil {
			logrus.Error(err)
			return
		}
	}
}
