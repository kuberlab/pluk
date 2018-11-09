package gc

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/jinzhu/gorm"
	"github.com/kuberlab/pluk/pkg/datasets"
	"github.com/kuberlab/pluk/pkg/db"
	"github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

const (
	gcInterval = time.Hour
	gcChunks   = time.Hour * 24
)

var (
	active *bool
	lock   = sync.RWMutex{}
)

func setActive() {
	if active == nil {
		ac := true
		active = &ac
	} else {
		*active = true
	}
}

func setInactive() {
	*active = false
}

func WaitGCCompleted() {
	if active != nil && !(*active) {
		return
	}

	timeout := time.NewTimer(time.Second * 30)
	ticker := time.NewTicker(time.Millisecond * 100)
	for {
		select {
		case <-ticker.C:
			if active != nil && !(*active) {
				return
			}
		case <-timeout.C:
			return
		}
	}
}

func Start() {
	utils.GCChan = make(chan string, 2)
	utils.GCClearChunks = make(chan string, 2)

	GoGC()

	ticker := time.NewTicker(gcInterval)
	tickerChunks := time.NewTicker(gcChunks)
	for {
		select {
		case <-ticker.C:
			GoGC()
		case msg := <-utils.GCChan:
			logrus.Infof("[GC] %v", msg)
			GoGC()
		case msg := <-utils.GCClearChunks:
			logrus.Infof("[ClearChunks] %v", msg)
			ClearChunks(db.DbMgr)
		case <-tickerChunks.C:
			ClearChunks(db.DbMgr)
		}
	}
}

func GoGC() {
	lock.Lock()
	defer lock.Unlock()
	setActive()
	defer setInactive()
	logrus.Info("[GC] Starting garbage collector...")
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
		err = deleteDatasetVersion(
			tx,
			&db.Dataset{Workspace: dsv.Workspace, Name: dsv.Name, Type: dsv.Type}, dsv.Version,
		)
		if err != nil {
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
	logrus.Infof("[GC] Done garbage collecting.")
}

func deleteDatasetVersion(mgr db.DataMgr, dataset *db.Dataset, version string) error {
	// Delete all files within this repo
	fileChunks, err := mgr.ListRelatedChunks(dataset.Type, dataset.Workspace, dataset.Name, version)
	if err != nil {
		return err
	}

	rows, err := mgr.DeleteRelatedFiles(dataset.Type, dataset.Workspace, dataset.Name, version)
	if err != nil {
		return err
	}
	var deleted = 0
	logrus.Infof("[GC] Deleted %v virtual files.", rows)
	for _, fc := range fileChunks {
		chunk, err := mgr.GetChunkByID(fc.ChunkID)
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				continue
			}
			return err
		}
		if datasets.CheckAndDeleteChunk(mgr, chunk) {
			deleted++
		}
		if deleted%500 == 0 && deleted != 0 {
			logrus.Infof("[GC] Deleted %v chunks.", deleted)
		}
	}
	logrus.Infof("[GC] Deleted %v chunks.", deleted)

	if version != "" {
		dsv, err := mgr.GetDatasetVersion(dataset.Type, dataset.Workspace, dataset.Name, version)
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
	sql := fmt.Sprintf("DELETE FROM dataset_versions WHERE workspace=? AND name=? AND type=?")
	err := mgr.DB().Exec(sql, d.Workspace, d.Name, d.Type).Error
	if err != nil {
		logrus.Error(err)
		return
	}
	mgr.DeleteDataset(d.ID)
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

	// Get list of available slaveDatasets: map[workspace + "__" + dataset_type][]dataset_name
	localDatasets := make(map[string]map[string]bool, 0)
	for _, ds := range vDatasets {
		if ds.Deleted {
			continue
		}
		if _, ok := localDatasets[ds.Workspace]; ok {
			localDatasets[ds.Workspace+"__"+ds.Type][ds.Name] = true
		} else {
			localDatasets[ds.Workspace+"__"+ds.Type] = map[string]bool{ds.Name: true}
		}
	}

	candidates := make([]types.Dataset, 0)
	for wsType, slaveDatasets := range localDatasets {
		ws, eType := wsAndType(wsType)
		remoteDatasets, err := io.MasterClient.ListEntities(eType, ws)
		if err != nil {
			logrus.Error(err)
			return
		}
		masterDatasetMap := make(map[string]bool)
		for _, ds := range remoteDatasets.Items {
			masterDatasetMap[ds.Name] = true
		}
		// Check if master has specific dataset on slave.
		// If it doesn't exists, then it was probably deleted.
		// Then we delete it as well on the slave.
		for slaveName := range slaveDatasets {
			if _, ok := masterDatasetMap[slaveName]; !ok {
				candidates = append(candidates, types.Dataset{Name: slaveName, Workspace: ws, Type: eType})
			}
		}
	}

	for _, candidate := range candidates {
		logrus.Infof("[GC] Delete %v %v/%v from slave", candidate.Type, candidate.Workspace, candidate.Name)
		if err = dsManager.DeleteDataset(candidate.Type, candidate.Workspace, candidate.Name, plukclient.NewInternalMasterClient(), false); err != nil {
			logrus.Error(err)
			return
		}
	}
}

func wsAndType(wsType string) (string, string) {
	arr := strings.Split(wsType, "__")
	return arr[0], arr[1]
}

type Answer struct {
	Size int64 `json:"size"`
}

func ClearChunks(db db.DataMgr) {
	var err error
	tx := db.DB().Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	err = filepath.Walk(utils.DataDir(), func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		hash := strings.TrimPrefix(path, "/data/")
		hash = strings.Replace(hash, "/", "", -1)

		size := info.Size()
		answer := Answer{}
		sql := fmt.Sprintf(`SELECT size from chunks WHERE hash='%v'`, hash)
		err = tx.Raw(sql).Scan(&answer).Error
		if err == gorm.ErrRecordNotFound {
			// Extra chunk / unneeded.
			os.Remove(path)
			logrus.Infof("[ClearChunks] Delete wrong chunk at %v", path)
			return nil
		}
		if err != nil {
			log.Println(err)
			return err
		}
		if answer.Size != 0 && answer.Size != size {
			os.Remove(path)
			logrus.Infof("[ClearChunks] Delete wrong chunk at %v", path)
		}

		return nil
	})
	if err != nil {
		log.Println(err)
		return
	}
	logrus.Info("[ClearChunks] Done.")
}
