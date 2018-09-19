package datasets

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/db"
	"github.com/kuberlab/pluk/pkg/utils"
)

func DeleteFile(mgr db.DataMgr, ws, dataset, version, path string) error {
	fileChunks, err := mgr.ListRelatedChunksForFiles(ws, dataset, version, path)
	if err != nil {
		return err
	}

	rows, err := mgr.DeleteFiles(ws, dataset, version, path)
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
		if CheckAndDeleteChunk(mgr, chunk) {
			deleted++
		}
	}
	logrus.Infof("Deleted %v chunks.", deleted)
	return nil
}

func CheckAndDeleteChunk(mgr db.DataMgr, chunk *db.Chunk) bool {
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