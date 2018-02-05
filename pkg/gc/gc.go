package gc

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pacak/pkg/pacakimpl"
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
	reps := make([]*db.File, 0)

	err := mgr.DB().Raw("SELECT DISTINCT repository_path from files").Scan(&reps).Error
	if err != nil {
		logrus.Error(err)
		return
	}

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
	for _, r := range reps {
		if !utils.Exists(r.RepositoryPath) {
			// Delete all files within this repo
			files, err = tx.ListFiles(db.File{RepositoryPath: r.RepositoryPath})
			if err != nil {
				logrus.Error(err)
				return
			}

			for _, f := range files {
				if checkAndDeleteFile(tx, f, false) {
					deleted++
				}
				if deleted%100 == 0 {
					logrus.Infof("Deleted %v objects", deleted)
				}
			}
		}
	}

	// Second: Iterate over files and see if the corresponding file for
	// specific version exists: smth. like 'git show <version>:<path>'
	// TODO: ^^
	endTx()

	// Third: See if there deleted dataset on master; delete those which don't exist on master
	// but exist on slave.
	if utils.HasMasters() {
		// Sync with master and delete obsolete datasets.
		gcFromMasters(mgr, reps)
	}
	logrus.Infof("Done garbage collecting. Deleted %v objects.", deleted)
}

func checkAndDeleteFile(mgr db.DataMgr, f *db.File, checkFile bool) bool {
	if checkFile {
		// Check if file for this version exists
		cmd := exec.Command(
			"git",
			"show",
			fmt.Sprintf("%v:%v", f.Version, strings.TrimPrefix(f.Path, "/")),
		)
		cmd.Dir = f.RepositoryPath
		_, err := cmd.Output()
		if err != nil {
			logrus.Debugf("git show output: %v", err)
		} else {
			// File exists
			return false
		}
	}
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
			return
		}

		// If there are no files in this directory, delete it.
		if len(remainFiles) == 0 {
			os.RemoveAll(dirName)
		}
	}
}

func gcFromMasters(mgr db.DataMgr, existingReps []*db.File) {
	gitIface := pacakimpl.NewGitInterface(utils.GitDir(), utils.GitLocalDir())
	dsManager := datasets.NewManager(gitIface)
	//var needCloseTx = true
	var err error
	//tx := mgr.Begin()
	//endTx := func() {
	//	if !needCloseTx {
	//		return
	//	}
	//	if err != nil {
	//		tx.Rollback()
	//	} else {
	//		tx.Commit()
	//	}
	//	needCloseTx = false
	//}
	//defer endTx()

	// Get list of available slaveDatasets: map[workspace][]dataset_name
	localDatasets := make(map[string]map[string]bool, 0)
	for _, r := range existingReps {
		if !utils.Exists(r.RepositoryPath) {
			continue
		}
		splitted := strings.Split(r.RepositoryPath, "/")
		name := splitted[len(splitted)-1]
		workspace := splitted[len(splitted)-2]
		if _, ok := localDatasets[workspace]; ok {
			localDatasets[workspace][name] = true
		} else {
			localDatasets[workspace] = map[string]bool{name: true}
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
