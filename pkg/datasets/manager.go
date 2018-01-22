package datasets

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pacak/pkg/pacakimpl"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

type Manager struct {
	git pacakimpl.GitInterface
}

func NewManager(git pacakimpl.GitInterface) *Manager {
	return &Manager{git: git}
}

func (m *Manager) ListDatasets(workspace string) []*Dataset {
	baseDir := path.Join(utils.GitDir(), workspace)

	dirs, err := ioutil.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*Dataset{}
		}
		logrus.Error(err)
		return []*Dataset{}
	}
	sets := make([]*Dataset, 0)

	for _, dir := range dirs {
		if dir.IsDir() {
			sets = append(sets, &Dataset{Dataset: types.Dataset{Name: dir.Name(), Workspace: workspace}, git: m.git})
		}
	}
	return sets
}

func (m *Manager) GetDataset(workspace, name string) *Dataset {
	datasets := m.ListDatasets(workspace)

	for _, d := range datasets {
		if d.Name == name {
			res := d
			res.InitRepo(false)
			return res
		}
	}

	// If none found, that means that it probably on master side.
	if !utils.HasMasters() {
		return nil
	}

	ds, err := plukclient.NewMultiClient().ListDatasets(workspace)
	if err != nil {
		logrus.Errorf("From master: %v", err)
		return nil
	}
	for _, d := range ds.Datasets {
		if d.Name == name {
			return &Dataset{Dataset: *d}
		}
	}

	return nil
}

func (m *Manager) NewDataset(workspace, name string) *Dataset {
	ds := &Dataset{Dataset: types.Dataset{Name: name, Workspace: workspace}, git: m.git}
	return ds
}

func (m *Manager) DeleteDataset(workspace, name string) error {
	repo := fmt.Sprintf("%v/%v", workspace, name)

	return m.git.DeleteRepository(repo)
}
