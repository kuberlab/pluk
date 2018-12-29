package datasets

import (
	"fmt"
	"net/http"

	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/db"
	"github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

type Manager struct {
	mgr db.DataMgr
	hub *types.Hub
}

func NewManager(mgr db.DataMgr, hub *types.Hub) *Manager {
	return &Manager{mgr: mgr, hub: hub}
}

func (m *Manager) ListDatasets(eType, workspace string) ([]*Dataset, error) {
	datasets, err := m.mgr.ListDatasets(db.Dataset{Type: eType, Workspace: workspace})
	if err != nil {
		return nil, err
	}
	sets := make([]*Dataset, 0)
	for _, d := range datasets {
		sets = append(sets, &Dataset{Dataset: d, mgr: m.mgr})
	}

	return sets, nil
}

func (m *Manager) GetDataset(eType, workspace, name string, master io.PlukClient) *Dataset {
	datasets, err := m.ListDatasets(eType, workspace)
	if err != nil {
		logrus.Errorf("List datasets: %v", err)
		return nil
	}

	for _, d := range datasets {
		if d.Name == name {
			res := d
			res.MasterClient = master
			return res
		}
	}

	// If none found, that means that it probably on master side.
	if !utils.HasMasters() || master == nil {
		return nil
	}

	ds, err := master.ListEntities(eType, workspace)
	if err != nil {
		logrus.Errorf("From master: %v", err)
		return nil
	}
	for _, d := range ds.Items {
		if d.Name == name {
			// Create locally.
			dataset, err := m.NewDataset(eType, workspace, name, nil)
			if err != nil {
				logrus.Error(err)
				return nil
			}
			dataset.MasterClient = master
			return dataset
		}
	}

	return nil
}

func (m *Manager) NewDataset(eType, workspace, name string, master io.PlukClient) (*Dataset, error) {
	dsDB, err := m.mgr.GetDataset(eType, workspace, name)
	if err != nil {
		dsDB = &db.Dataset{Type: eType, Workspace: workspace, Name: name}
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
	ds := &Dataset{Dataset: dsDB, mgr: m.mgr, MasterClient: master}
	return ds, nil
}

func (m *Manager) ForkDataset(src types.Dataset, target types.Dataset, master io.PlukClient) (*Dataset, error) {
	_, err := m.mgr.GetDataset(target.DType, target.Workspace, target.Name)
	if err == nil {
		msg := fmt.Sprintf(
			"%v %v/%v already exists. Please delete it first and try again.",
			strings.Title(target.DType), target.Workspace, target.Name,
		)
		return nil, errors.NewStatus(409, msg)
	}

	source := m.GetDataset(src.DType, src.Workspace, src.Name, master)
	if source == nil {
		msg := fmt.Sprintf("Probably %v %v/%v doesn't exist", src.DType, src.Workspace, src.Name)
		return nil, errors.NewStatus(404, msg)
	}

	ds, err := m.NewDataset(target.DType, target.Workspace, target.Name, master)
	if err != nil {
		return nil, err
	}

	sourceVersions, err := source.Versions()
	if err != nil {
		return nil, err
	}

	for _, ver := range sourceVersions {
		if !ver.Editing {
			if _, err = source.CloneVersionTo(ds, ver.Version, ver.Version, ver.Message); err != nil {
				return nil, err
			}
			if _, err = ds.CommitVersion(ver.Version, ver.Message); err != nil {
				return nil, err
			}
		}
	}

	return ds, nil
}

func (m *Manager) DeleteDataset(eType, workspace, name string, master io.PlukClient, force bool) error {
	ds, err := m.mgr.GetDataset(eType, workspace, name)
	if err != nil {
		return errors.NewStatus(
			http.StatusNotFound,
			fmt.Sprintf("%v %v not found: %v", strings.Title(eType), name, err),
		)
	}
	dsvs, err := m.mgr.ListDatasetVersions(
		db.DatasetVersion{Name: name, Workspace: workspace, Type: eType},
	)
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

	if utils.HasMasters() && master != nil {
		_ = master.DeleteEntity(ds.Type, workspace, name, force)
	}

	if force {
		utils.GCChan <- fmt.Sprintf("Clean dataset %v/%v", workspace, name)
	}

	// Push message about deleting dataset here
	m.PushMessageDataset(&types.Dataset{Workspace: ds.Workspace, Name: ds.Name, DType: ds.Type})

	return nil
}

func (m *Manager) PushMessageDataset(ds *types.Dataset) {
	if m.hub == nil || ds == nil {
		return
	}

	msg := *ds
	m.hub.Push(&msg)
}

func (m *Manager) PushMessageVersion(dsv *types.Version) {
	if m.hub == nil || dsv == nil {
		return
	}

	msg := *dsv
	m.hub.Push(&msg)
}
