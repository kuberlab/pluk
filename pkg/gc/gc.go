package gc

import (
	"fmt"
	"os"
	"os/exec"
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
	mgr := db.DbMgr
	reps := make([]*db.File, 0)

	err := mgr.DB().Raw("SELECT DISTINCT repository_path from files").Scan(&reps).Error
	if err != nil {
		logrus.Error(err)
		return
	}

	// First: check if repo exists.
	for _, r := range reps {
		if !utils.Exists(r.RepositoryPath) {
			// Delete all files within this repo
			files, err := mgr.ListFiles(db.File{RepositoryPath: r.RepositoryPath})
			if err != nil {
				logrus.Error(err)
				return
			}

			for _, f := range files {
				checkAndDeleteFile(mgr, f, false)
			}
		}
	}

	// Second: Iterate over files and see if the corresponding file for
	// specific version exists: smth. like 'git show <version>:<path>'
}

func checkAndDeleteFile(mgr db.DataMgr, f *db.File, checkFile bool) {
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
			return
		}
	}
	mgr.DeleteFile(f.ID)

	chunkIDs, err := mgr.ListFileChunks(db.FileChunk{FileID: f.ID})
	if err != nil {
		logrus.Error(err)
		return
	}
	for _, chunkID := range chunkIDs {
		chunk, err := mgr.GetChunkByID(chunkID.ChunkID)
		if err != nil {
			logrus.Error(err)
			return
		}
		checkAndDeleteChunk(mgr, chunk, f)
	}
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
	}
}
