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
	"github.com/kuberlab/pluk/pkg/db"
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
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

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
