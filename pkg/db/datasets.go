package db

import "fmt"

type DatasetMgr interface {
	CreateDataset(dataset *Dataset) error
	UpdateDataset(dataset *Dataset) (*Dataset, error)
	RecoverDataset(dataset *Dataset) error
	GetDataset(dsType, workspace, name string) (*Dataset, error)
	GetDatasetByID(datasetID uint) (*Dataset, error)
	ListDatasets(filter Dataset) ([]*Dataset, error)
	DeleteDataset(id uint) error
}

type Dataset struct {
	BaseModel
	ID        uint   `json:"id" sql:"AUTO_INCREMENT" gorm:"primary_key"`
	Workspace string `json:"workspace" gorm:"index:idx_workspace_type"`
	Name      string `json:"name" gorm:"index:idx_name"`
	Type      string `json:"type" gorm:"index:idx_workspace_type"`
	Deleted   bool   `json:"deleted"`
}

func (mgr *DatabaseMgr) CreateDataset(dataset *Dataset) error {
	return mgr.db.Create(dataset).Error
}

func (mgr *DatabaseMgr) UpdateDataset(dataset *Dataset) (*Dataset, error) {
	err := mgr.db.Save(dataset).Error
	return dataset, err
}

func (mgr *DatabaseMgr) RecoverDataset(dataset *Dataset) error {
	sql := fmt.Sprintf("UPDATE datasets SET deleted=0 where name='%v' AND workspace='%v'", dataset.Name, dataset.Workspace)
	return mgr.db.Exec(sql).Error
}

func (mgr *DatabaseMgr) GetDataset(dsType, workspace, name string) (*Dataset, error) {
	var dataset = Dataset{}
	err := mgr.db.First(&dataset, Dataset{Type: dsType, Workspace: workspace, Name: name}).Error
	return &dataset, err
}

func (mgr *DatabaseMgr) GetDatasetByID(datasetID uint) (*Dataset, error) {
	var dataset = Dataset{}
	err := mgr.db.First(&dataset, Dataset{ID: datasetID}).Error
	return &dataset, err
}

func (mgr *DatabaseMgr) ListDatasets(filter Dataset) ([]*Dataset, error) {
	var datasets = make([]*Dataset, 0)
	db := mgr.db
	if !filter.Deleted {
		db = db.Where("deleted=0")
	}
	err := db.Find(&datasets, filter).Error
	return datasets, err
}

func (mgr *DatabaseMgr) DeleteDataset(id uint) error {
	return mgr.db.Delete(Dataset{}, Dataset{ID: id}).Error
}
