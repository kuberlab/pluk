package io

import (
	"bytes"
	"crypto/sha512"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
	"golang.org/x/sync/semaphore"
)

var queue = semaphore.NewWeighted(5)

type ChunkedReader struct {
	ChunkSize int
	reader    io.Reader
}

func NewChunkedReader(chunkSize int, reader io.Reader) *ChunkedReader {
	return &ChunkedReader{
		ChunkSize: chunkSize,
		reader:    reader,
	}
}

func (c *ChunkedReader) NextChunk() ([]byte, string, error) {
	data := make([]byte, c.ChunkSize)

	n, err := c.reader.Read(data)
	if n > 0 {
		res := data[:n]
		sum := sha512.Sum512(res)
		return res, fmt.Sprintf("%x", sum[:]), nil
	}
	if err != nil {
		return nil, "", err
	}
	return nil, "", io.EOF
}

func CheckChunk(hash string) (*types.ChunkCheck, error) {
	size, exists := CheckLocalChunk(hash)

	// Check chunk on master
	if utils.HasMasters() {
		check, err := MasterClient.CheckChunk(hash)
		if err != nil {
			return nil, err
		}
		sizeM := check.Size
		existsM := check.Exists

		// If chunk on master has a smaller size, then mark it with this size
		// which will be treated as wrong and will lead to uploading that chunk.
		if sizeM < size {
			size = sizeM
		}
		// For ignoring uploading chunk, it must exists on master as well.
		exists = exists && existsM
	}
	return &types.ChunkCheck{Hash: hash, Exists: exists, Size: size}, nil
}

func CheckLocalChunk(hash string) (int64, bool) {
	filePath := utils.GetHashedFilename(hash)
	stat, err := os.Stat(filePath)
	if err != nil {
		return 0, false
	}
	return stat.Size(), err == nil
}

func GetChunk(hash string) (reader ReaderInterface, err error) {
	chunkPath := utils.GetHashedFilename(hash)
	reader, err = os.Open(chunkPath)
	if err != nil {
		if reader != nil {
			reader.Close()
		}
		if os.IsNotExist(err) && utils.HasMasters() {
			// Read from master
			//logrus.Debugf("download")
			//t := time.Now()
			readerRaw, err := MasterClient.DownloadChunk(hash)

			if err != nil {
				return nil, err
			}
			if utils.SaveChunks() {
				data, err := ioutil.ReadAll(readerRaw)
				if err != nil {
					return nil, err
				}
				//logrus.Debugf("download complete! %v", time.Since(t))
				readerRaw.Close()
				SaveChunk(hash, ioutil.NopCloser(bytes.NewBuffer(data)), false)
				return NewChunkReaderFromData(data), nil
			} else {
				data, err := ioutil.ReadAll(readerRaw)
				if err != nil {
					return nil, err
				}
				//logrus.Debugf("download complete! %v", time.Since(t))
				readerRaw.Close()
				return NewChunkReaderFromData(data), nil
				//if err != nil {
				//	return nil, err
				//}
				//return reader, nil
			}
		} else {
			return nil, err
		}
	}
	return reader, err
}

func SaveChunk(hash string, data io.ReadCloser, sendToMaster bool) error {
	//logrus.Debugf("Save")
	//t := time.Now()
	filePath := utils.GetHashedFilename(hash)

	splitted := strings.Split(filePath, "/")
	baseDir := splitted[:len(splitted)-1]

	if err := os.MkdirAll(strings.Join(baseDir, "/"), os.ModePerm); err != nil {
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	logrus.Debugf("Created %v", filePath)

	defer file.Close()

	buf := bytes.NewBuffer([]byte{})
	var written int64
	var writer io.Writer = file
	if utils.HasMasters() && sendToMaster {
		// If we have masters, then also write to buf in order to use it for further push.
		writer = io.MultiWriter(writer, buf)
	}
	written, err = io.Copy(writer, data)
	if err != nil {
		return err
	}
	data.Close()

	logrus.Debugf("Written %v bytes.", written)

	if utils.HasMasters() && sendToMaster {
		// TODO: decide whether it can go in async
		return MasterClient.SaveChunk(hash, buf.Bytes())
	}
	//logrus.Debugf("Save complete! %v", time.Since(t))
	return nil
}
