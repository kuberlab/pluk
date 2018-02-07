package datasets

import (
	"fmt"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/db"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/kuberlab/pluk/pkg/utils"
)

type Manager struct {
	mgr db.DataMgr
}

func NewManager(mgr db.DataMgr) *Manager {
	return &Manager{mgr: mgr}
}

func (m *Manager) ListDatasets(workspace string) []*Dataset {
	datasets, err := m.mgr.ListDatasets(db.Dataset{Workspace: workspace})
	if err != nil {
		logrus.Error(err)
		return []*Dataset{}
	}
	sets := make([]*Dataset, 0)
	for _, d := range datasets {
		sets = append(sets, &Dataset{Dataset: &db.Dataset{Name: d.Name, Workspace: workspace}, mgr: m.mgr})
	}

	return sets
}

func (m *Manager) GetDataset(workspace, name string) *Dataset {
	datasets := m.ListDatasets(workspace)

	for _, d := range datasets {
		if d.Name == name {
			res := d
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
			// Create locally.
			dsDB := &db.Dataset{Name: d.Name, Workspace: d.Workspace}
			err := m.mgr.CreateDataset(dsDB)
			if err != nil {
				logrus.Error(err)
				return nil
			}
			return &Dataset{Dataset: dsDB, mgr: m.mgr}
		}
	}

	return nil
}

func (m *Manager) NewDataset(workspace, name string) (*Dataset, error) {
	dsDB, err := m.mgr.GetDataset(workspace, name)
	if err != nil {
		dsDB = &db.Dataset{Workspace: workspace, Name: name}
		if err = m.mgr.CreateDataset(dsDB); err != nil {
			return nil, err
		}
	} else if dsDB.Deleted {
		// Recover it.
		dsDB.Deleted = false
		if err = m.mgr.RecoverDataset(dsDB); err != nil {
			return nil, err
		}
	}
	ds := &Dataset{Dataset: dsDB, mgr: m.mgr}
	return ds, nil
}

func (m *Manager) DeleteDataset(workspace, name string) error {
	ds, err := m.mgr.GetDataset(workspace, name)
	if err != nil {
		return errors.NewStatus(http.StatusNotFound, fmt.Sprintf("Dataset %v not found: %v", name, err))
	}
	dsvs, err := m.mgr.ListDatasetVersions(db.DatasetVersion{Name: name, Workspace: workspace})
	if err != nil {
		return err
	}

	for _, dsv := range dsvs {
		dsv.Deleted = true
		if _, err = m.mgr.UpdateDatasetVersion(dsv); err != nil {
			return err
		}
	}
	ds.Deleted = true
	if _, err = m.mgr.UpdateDataset(ds); err != nil {
		return err
	}

	return nil
}
