package dataset

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"time"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pacak/pkg/pacakimpl"
	"github.com/kuberlab/pluk/pkg/utils"
)

const (
	Author        = "pluk"
	AuthorEmail   = "pluk@kuberlab.io"
	defaultBranch = "master"
)

type FileStructure struct {
	Files []*HashedFile `json:"files"`
}

type HashedFile struct {
	Path     string      `json:"path"`
	Hashes   []string    `json:"hashes"`
	Mode     os.FileMode `json:"mode"`
	ModeTime time.Time   `json:"mode_time"`
}

func SaveDataset(git pacakimpl.GitInterface, structure FileStructure, workspace string, name string, version string, comment string) error {
	repo := fmt.Sprintf("%v/%v", workspace, name)
	pacakRepo, err := initRepo(git, repo, true)
	if err != nil {
		return err
	}

	// Make absolute path for hashes and build gitFiles
	files := make([]pacakimpl.GitFile, 0)
	for _, f := range structure.Files {
		paths := make([]string, 0)
		for _, h := range f.Hashes {
			filePath := utils.GetHashedFilename(h)
			paths = append(paths, filePath)
		}
		files = append(files, pacakimpl.GitFile{Path: f.Path, Data: []byte(strings.Join(paths, "\n"))})
	}
	logrus.Infof("Saving data for %v/%v...", workspace, name)

	commit, err := pacakRepo.Save(getCommitter(), buildMessage(version, comment), defaultBranch, defaultBranch, files)
	if err != nil {
		return err
	}
	logrus.Infof("Saved as commit %v.", commit)
	return nil
}

func CheckChunk(hash string) bool {
	filePath := utils.GetHashedFilename(hash)
	_, err := os.Stat(filePath)
	return err == nil
}

func SaveChunk(hash string, data io.ReadCloser) error {
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

	written, err := io.Copy(file, data)
	if err != nil {
		return err
	}
	data.Close()

	logrus.Debugf("Written %v bytes.", written)
	return nil
}

func GetDataset(git pacakimpl.GitInterface, workspace string, name string, version string, resp *restful.Response) error {
	repo := fmt.Sprintf("%v/%v", workspace, name)
	pacakRepo, err := initRepo(git, repo, false)
	if err != nil {
		return err
	}
	// Need to checkout needed commit and archive the chunked data.
	commits, err := pacakRepo.Commits(defaultBranch, func(message string) bool {
		msg := parseMessage(message)
		if msg == nil {
			return false
		}
		return msg.Version == version
	})
	if err != nil {
		return err
	}
	if len(commits) < 1 {
		return errors.NewStatus(404, fmt.Sprintf("Version %v not found.", version))
	}

	commitID := commits[0].ID
	logrus.Infof("Found commit %v.", commitID)

	if err = pacakRepo.Checkout(commitID); err != nil {
		return err
	}
	defer pacakRepo.Checkout(defaultBranch)

	// Build archive.
	return WriteTarGz(fmt.Sprintf("%v/%v/%v", utils.GitLocalDir(), workspace, name), resp)
}

func Versions(git pacakimpl.GitInterface, workspace string, name string) ([]string, error) {
	repo := fmt.Sprintf("%v/%v", workspace, name)
	pacakRepo, err := initRepo(git, repo, false)

	if err != nil {
		return nil, err
	}

	var versions = make([]string, 0)
	_, err = pacakRepo.Commits(defaultBranch, func(message string) bool {
		msg := parseMessage(message)
		if msg == nil {
			return false
		}
		versions = append(versions, msg.Version)

		return true
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
