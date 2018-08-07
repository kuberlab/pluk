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
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

const (
	gcInterval = time.Hour
)

func Start() {
	GoGC()

	ticker := time.NewTicker(gcInterval)
	utils.GCChan = make(chan string)
	for {
		select {
		case <-ticker.C:
			GoGC()
		case msg := <-utils.GCChan:
			logrus.Infof("[GC] %v", msg)
			GoGC()
		}
	}
}

func GoGC() {
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
	// Done: Using WAL mode (Write ahead log) for SQLite.
	tx := mgr.Begin()
	endTx := func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}
	defer endTx()

	// First: check if repo exists.
	for _, ds := range vDatasets {
		if err = deleteDatasetVersion(tx, ds, ""); err != nil {
			logrus.Error(err)
			return
		}
	}

	// Second: Iterate over versions and see if the corresponding version deleted.
	endTx()
	tx = mgr.Begin()
	deletedVersions, err := mgr.ListDatasetVersions(db.DatasetVersion{Deleted: true})
	if err != nil {
		logrus.Error(err)
		return
	}
	for _, dsv := range deletedVersions {
		if err = deleteDatasetVersion(tx, &db.Dataset{Workspace: dsv.Workspace, Name: dsv.Name}, dsv.Version); err != nil {
			logrus.Error(err)
			return
		}
	}

	// Third: See if there deleted dataset on master; delete those which don't exist on master
	// but exist on slave.
	if utils.HasMasters() {
		// Sync with master and delete obsolete datasets.
		gcFromMasters(mgr)
	}
	logrus.Infof("Done garbage collecting.")
}

func deleteDatasetVersion(mgr db.DataMgr, dataset *db.Dataset, version string) error {
	// Delete all files within this repo
	fileChunks, err := mgr.ListRelatedChunks(dataset.Workspace, dataset.Name, version)
	if err != nil {
		return err
	}

	rows, err := mgr.DeleteRelatedFiles(dataset.Workspace, dataset.Name, version)
	if err != nil {
		return err
	}
	var deleted = 0
	logrus.Infof("Deleted %v virtual files.", rows)
	for _, fc := range fileChunks {
		chunk, err := mgr.GetChunkByID(fc.ChunkID)
		if err != nil {
			return err
		}
		if checkAndDeleteChunk(mgr, chunk) {
			deleted++
		}
		if deleted%500 == 0 && deleted != 0 {
			logrus.Infof("Deleted %v chunks.", deleted)
		}
	}
	logrus.Infof("Deleted %v chunks.", deleted)

	if version != "" {
		dsv, err := mgr.GetDatasetVersion(dataset.Workspace, dataset.Name, version)
		if err != nil {
			return err
		}
		if err = mgr.DeleteDatasetVersion(dsv.ID); err != nil {
			return err
		}
	} else {
		deleteDataset(mgr, dataset)
	}

	return nil
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

func checkAndDeleteChunk(mgr db.DataMgr, chunk *db.Chunk) bool {
	// See if there are more connections on this chunk
	deleted := false
	remain, err := mgr.ListFileChunks(db.FileChunk{ChunkID: chunk.ID})
	if err != nil {
		logrus.Error(err)
		return false
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
		deleted = true
	}
	return deleted
}

func gcFromMasters(mgr db.DataMgr) {
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
		if err = dsManager.DeleteDataset(candidate.Workspace, candidate.Name, plukclient.NewInternalMasterClient(), false); err != nil {
			logrus.Error(err)
			return
		}
	}
}
