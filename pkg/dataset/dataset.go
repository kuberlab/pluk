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
	Size     uint64      `json:"size"`
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
		// Virtual file structure:
		// <size (uint64)>
		// <chunk path1>
		// <chunk path2>
		// ..
		// <chunk pathN>
		//
		content := fmt.Sprintf("%v\n%v", f.Size, strings.Join(paths, "\n"))
		files = append(files, pacakimpl.GitFile{Path: f.Path, Data: []byte(content)})
	}
	logrus.Infof("Saving data for %v/%v:%v...", workspace, name, version)

	commit, err := pacakRepo.Save(getCommitter(), buildMessage(version, comment), defaultBranch, defaultBranch, files)
	if err != nil {
		return err
	}
	logrus.Infof("Saved as commit %v.", commit)

	if err = pacakRepo.PushTag(version, commit, true); err != nil {
		return err
	}
	logrus.Infof("Created tag %v.", version)

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
	logrus.Infof("Checkout tag %v.", version)

	if !pacakRepo.IsTagExists(version) {
		return errors.NewStatus(404, fmt.Sprintf("Version %v not found for dataset %v.", version, name))
	}

	if err = pacakRepo.Checkout(version); err != nil {
		return err
	}
	defer pacakRepo.Checkout(defaultBranch)

	// Build archive.
	return WriteTarGz(fmt.Sprintf("%v/%v/%v", utils.GitLocalDir(), workspace, name), resp)
}

func DeleteDataset(git pacakimpl.GitInterface, workspace string, name string) error {
	repo := fmt.Sprintf("%v/%v", workspace, name)

	return git.DeleteRepository(repo)
}

func Versions(git pacakimpl.GitInterface, workspace string, name string) ([]string, error) {
	repo := fmt.Sprintf("%v/%v", workspace, name)
	pacakRepo, err := initRepo(git, repo, false)

	if err != nil {
		return nil, err
	}

	return pacakRepo.TagList()
}

func Datasets(workspace string) ([]string, error) {
	baseDir := path.Join(utils.GitDir(), workspace)

	dirs, err := ioutil.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
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
