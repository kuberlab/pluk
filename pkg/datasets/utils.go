package datasets

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/db"
	"github.com/kuberlab/pluk/pkg/utils"
)

var (
	active   = false
	lock     sync.RWMutex
	deleteCh = make(chan string, 5000)

	ChunkActive   = false
	ChunkLock     sync.RWMutex
	chunksDB      = make(chan db.Chunk, 0)
	chunksTrigger = make(chan db.DataMgr, 10)
	chunksDeleted = make(chan int64, 10)
	chunksBuf     = make([]db.Chunk, 0)
	deleteBatch   = 250
)

func SendDeletePath(path string) {
	deleteCh <- path
}

func RunDeleteLoop() {
	lock.Lock()
	if active {
		return
	}
	active = true
	lock.Unlock()
	for path := range deleteCh {
		_ = os.Remove(path)

		dirName := filepath.Dir(path)
		remainFiles, err := ioutil.ReadDir(dirName)
		if err != nil {
			//logrus.Error(err)
		}

		// If there are no files in this directory, delete it.
		if len(remainFiles) == 0 {
			_ = os.RemoveAll(dirName)
		}
	}
}

func RunChunkDBDeleteLoop() {
	ChunkLock.Lock()
	if ChunkActive {
		return
	}
	ChunkActive = true
	ChunkLock.Unlock()
	//ticker := time.NewTicker(time.Second * 15)
	for {
		select {
		case chunk := <-chunksDB:
			//
			//fmt.Println("receive id ", chunk.ID)
			chunksBuf = append(chunksBuf, chunk)
		case mgr := <-chunksTrigger:
			var deleted int64 = 0
			if len(chunksBuf) != 0 {
				// Flush buffer and delete chunks
				// chunk buffer into smaller slices
				chunkSize := deleteBatch

				for i := 0; i < len(chunksBuf); i += chunkSize {
					end := i + chunkSize

					if end > len(chunksBuf) {
						end = len(chunksBuf)
					}

					deleted += deleteChunks(mgr, chunksBuf[i:end])
				}

				//deleteChunks(mgr, chunksBuf)
				chunksBuf = nil
			}
			chunksDeleted <- deleted
		}
	}
}

func TriggerDeleteChunks(mgr db.DataMgr) int64 {
	chunksTrigger <- mgr
	return <-chunksDeleted
}

func deleteChunks(mgr db.DataMgr, chunks []db.Chunk) int64 {
	fileChunks, err := mgr.ListFileChunksByChunks(chunks)
	if err != nil {
		logrus.Error(err)
		return 0
	}

	chunkMap := make(map[uint]db.Chunk)
	// initialize with true (all chunks are scheduled to delete)
	for _, c := range chunks {
		chunkMap[c.ID] = c
	}
	for _, fc := range fileChunks {
		// If chunk has a connection with fileChunk,
		// then it must not be deleted
		delete(chunkMap, fc.ChunkID)
	}

	deleteChunks := make([]db.Chunk, 0)
	for _, chunk := range chunkMap {
		deleteChunks = append(deleteChunks, chunk)
		path := utils.GetHashedFilename(chunk.Hash, chunk.Version)

		// Send to delete
		deleteCh <- path
	}
	if len(deleteChunks) != 0 {
		_ = mgr.DeleteChunks(deleteChunks)
		//logrus.Infof("Deleted %v chunks.", len(deleteChunks))
	}
	return int64(len(deleteChunks))
}

func DeleteFiles(mgr db.DataMgr, eType, ws, dataset, version, prefix string, preciseName, strict bool) error {
	rawFiles, err := mgr.GetRawFiles(eType, ws, dataset, version, prefix, "", preciseName)
	if err != nil {
		return err
	}

	rows, err := mgr.DeleteFiles(eType, ws, dataset, version, prefix, preciseName)
	if err != nil {
		return err
	}
	logrus.Infof("Deleted %v virtual files.", rows)

	if len(rawFiles) == 0 && strict {
		return errors.NewStatus(
			http.StatusNotFound,
			fmt.Sprintf("Path %v not found in %v %v/%v:%v", eType, prefix, ws, dataset, version),
		)
	}

	//var deleted = 0
	for _, raw := range rawFiles {
		//chunk, err := mgr.GetChunkByID(raw.ChunkID)
		//if err != nil {
		//	return err
		//}
		chunk := &db.Chunk{Hash: raw.Hash, Size: raw.ChunkSize, ID: raw.ChunkID}
		CheckAndDeleteChunk(mgr, chunk)
		//if CheckAndDeleteChunk(mgr, chunk) {
		//	deleted++
		//}
	}
	deleted := TriggerDeleteChunks(mgr)
	if deleted != 0 {
		logrus.Infof("Deleted %v chunks.", deleted)
	}
	return nil
}

func CheckAndDeleteChunk(mgr db.DataMgr, chunk *db.Chunk) {
	chunksDB <- *chunk
	// See if there are more connections on this chunk
	//deleted := false
	//remain, err := mgr.ListFileChunks(db.FileChunk{ChunkID: chunk.ID})
	//if err != nil {
	//	logrus.Error(err)
	//	return false
	//}
	//// If there are no connections, completely delete this chunk.
	//if len(remain) == 0 {
	//	path := utils.GetHashedFilename(chunk.Hash)
	//	_ = mgr.DeleteChunk(chunk.ID)
	//
	//	// Send to delete
	//	deleteCh <- path
	//
	//	deleted = true
	//}
	//return deleted
}
