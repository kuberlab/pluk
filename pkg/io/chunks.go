package io

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

type ChunkedReader struct {
	ChunkSize int
	reader    io.Reader
	Timer     time.Duration
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
		t := time.Now()
		sum := utils.CalcHash(res)
		c.Timer += time.Since(t)
		return res, sum, nil
	}
	if err != nil {
		return nil, "", err
	}
	return nil, "", io.EOF
}

func CheckChunk(hash string, version byte) (*types.ChunkCheck, error) {
	size, exists := CheckLocalChunk(hash, version)

	// Check chunk on master
	if utils.HasMasters() {
		check, err := MasterClient.CheckChunk(hash, version)
		if err != nil {
			return nil, err
		}
		sizeM := check.Size
		existsM := check.Exists

		// If chunk on master has a smaller size, then mark it with this size
		// which will be treated as wrong and will lead to uploading that chunk.
		if sizeM != size {
			size = sizeM
		}
		// For ignoring uploading chunk, it must exists on master as well.
		exists = exists && existsM
	}
	return &types.ChunkCheck{Hash: hash, Exists: exists, Size: size}, nil
}

func CheckLocalChunk(hash string, version byte) (int64, bool) {
	filePath := utils.GetHashedFilename(hash, version)
	stat, err := os.Stat(filePath)
	if err != nil {
		return 0, false
	}
	return stat.Size(), err == nil
}

func GetChunkByHash(hash string, version byte) (reader io.ReadCloser, err error) {
	chunkPath := utils.GetHashedFilename(hash, version)
	return GetChunk(chunkPath, version)
}

func GetChunk(chunkPath string, version byte) (reader ReaderInterface, err error) {
	reader, err = os.Open(chunkPath)
	if err != nil {
		hash, _ := utils.GetHashFromPath(chunkPath)
		if reader != nil {
			reader.Close()
		}
		if os.IsNotExist(err) && utils.HasMasters() {
			// Read from master
			//logrus.Debugf("download")
			//t := time.Now()

			getData := func(hash string, version byte, buffer *bytes.Buffer) error {
				buffer.Reset()
				check, err := MasterClient.CheckChunk(hash, version)
				if err != nil {
					return err
				}
				readerRaw, err := MasterClient.DownloadChunk(hash, version)

				if err != nil {
					return err
				}
				data, err := ioutil.ReadAll(readerRaw)
				if err != nil {
					return err
				}
				readerRaw.Close()
				if int64(len(data)) != check.Size {
					return fmt.Errorf("Downloaded chunk size mismatch with real chunk size")
				}
				buffer.Write(data)
				return nil
			}

			buf := bytes.NewBuffer([]byte{})
			_, err := utils.Retry("get chunk", 0.1, 30, getData, hash, byte(version), buf)
			if err != nil {
				logrus.Warningf("Failed get chunk: %v", err)
			}
			data := buf.Bytes()

			if utils.SaveChunks() {
				//logrus.Debugf("download complete! %v", time.Since(t))
				err = SaveChunk(hash, version, ioutil.NopCloser(bytes.NewBuffer(data)), false)
				if err != nil {
					logrus.Errorf("Could not save chunk: %v", err)
				}
			}
			return NewChunkReaderFromData(data), nil
		} else {
			return nil, err
		}
	}
	return reader, err
}

func SaveChunk(hash string, version byte, data io.ReadCloser, sendToMaster bool) error {
	//logrus.Debugf("Save")
	//t := time.Now()
	filePath := utils.GetHashedFilename(hash, version)

	splitted := strings.Split(filePath, "/")
	baseDir := splitted[:len(splitted)-1]

	if err := os.MkdirAll(strings.Join(baseDir, "/"), os.ModePerm); err != nil {
		data.Close()
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		data.Close()
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
		data.Close()
		return err
	}
	data.Close()

	logrus.Debugf("Written %v bytes.", written)

	if utils.HasMasters() && sendToMaster {
		// TODO: decide whether it can go in async
		return MasterClient.SaveChunk(hash, buf.Bytes(), version)
	}
	//logrus.Debugf("Save complete! %v", time.Since(t))
	return nil
}
