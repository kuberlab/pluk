package dataset

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pacak/pkg/pacakimpl"
	"github.com/kuberlab/pluk/pkg/utils"
)

const (
	Author        = "pluk"
	AuthorEmail   = "pluk@kuberlab.io"
	ChunkByteSize = 1048576
	defaultBranch = "master"
)

func SaveDataset(git pacakimpl.GitInterface, targz io.ReadCloser, workspace string, name string, version string, comment string) error {
	files := make([]pacakimpl.GitFile, 0)

	saveAndPopulateFiles := func(name string, data []byte) error {
		chunksCount := int64(math.Ceil(float64(len(data)) / float64(ChunkByteSize)))
		var hashes = make([]string, 0)
		for i := int64(0); i < chunksCount; i++ {
			start := i * ChunkByteSize
			end := (i + 1) * ChunkByteSize
			if end >= int64(len(data)) {
				end = int64(len(data))
			}
			chunkHash := utils.CalcHash(data[start:end])

			hashDir := chunkHash[:8]
			hashFile := chunkHash[8:]

			targetDir := path.Join(utils.DataDir(), hashDir)
			fullPath := targetDir + "/" + hashFile
			hashes = append(hashes, fullPath)

			logrus.Debugf("Full hash path: %v", fullPath)
			os.MkdirAll(targetDir, os.ModePerm)
			// Save hash file with chunk data
			if err := ioutil.WriteFile(fullPath, data[start:end], 0666); err != nil {
				return err
			}
		}
		files = append(
			files,
			pacakimpl.GitFile{
				Path: name,
				Data: []byte(strings.Join(hashes, "\n")),
			},
		)
		return nil
	}
	repo := fmt.Sprintf("%v/%v", workspace, name)
	pacakRepo, err := initRepo(git, repo, true)
	if err != nil {
		return err
	}

	if err = iterateOverTarGz(targz, saveAndPopulateFiles); err != nil {
		return err
	}
	logrus.Infof("Saving data for %v/%v...", workspace, name)

	commit, err := pacakRepo.Save(getCommitter(), buildMessage(version, comment), defaultBranch, defaultBranch, files)
	if err != nil {
		return err
	}
	logrus.Infof("Saved as commit %v.", commit)
	return nil
}

func GetDataset(git pacakimpl.GitInterface, workspace string, name string, version string) {
	// find all chunks in dataset repo, squash them and pack into tar.gz
}

func Versions(git pacakimpl.GitInterface, workspace string, name string) ([]string, error) {
	repo := fmt.Sprintf("%v/%v", workspace, name)
	pacakRepo, err := initRepo(git, repo, false)

	if err != nil {
		return nil, err
	}

	var versions = make([]string, 0)
	_, err = pacakRepo.Commits(defaultBranch, func(message string) bool {
		lines := strings.Split(message, "\n")
		for _, line := range lines {
			splitted := strings.Split(line, ": ")
			if len(splitted) != 2 {
				return false
			}
			if splitted[0] == "Version" {
				versions = append(versions, splitted[1])
				return true
			}
		}

		return false
	})

	return versions, err
}

func Datasets(workspace string) ([]string, error) {
	baseDir := path.Join(utils.GitDir(), workspace)

	dirs, err := ioutil.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}
	sets := make([]string, 0)

	for _, dir := range dirs {
		if dir.IsDir() {
			sets = append(sets, dir.Name())
		}
	}
	return sets, nil
}

func buildMessage(version, comment string) string {
	return fmt.Sprintf("Version: %v\nComment: %v", version, comment)
}
