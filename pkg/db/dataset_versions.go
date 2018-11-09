package db

import (
	"fmt"
)

type DatasetVersionVersionMgr interface {
	CreateDatasetVersion(datasetVersionVersion *DatasetVersion) error
	UpdateDatasetVersion(datasetVersion *DatasetVersion) (*DatasetVersion, error)
	GetDatasetVersion(dsType, workspace, name, version string) (*DatasetVersion, error)
	GetDatasetVersionByID(datasetVersionID uint) (*DatasetVersion, error)
	ListDatasetVersions(filter DatasetVersion) ([]*DatasetVersion, error)
	DeleteDatasetVersion(id uint) error
	RecoverDatasetVersion(dsv *DatasetVersion) error
	CommitVersion(dsType, workspace, name, version, message string) (*DatasetVersion, error)
	UpdateDatasetVersionSize(dsType, workspace, name, version string) error
}

type DatasetVersion struct {
	BaseModel
	ID        uint   `sql:"AUTO_INCREMENT" gorm:"primary_key"`
	Workspace string `json:"workspace" gorm:"index:idx_workspace_name_type"`
	Name      string `json:"name" gorm:"index:idx_workspace_name_type"`
	Version   string `json:"version" gorm:"index:idx_version"`
	Message   string `json:"message"`
	Type      string `json:"type" gorm:"index:idx_workspace_name_type"`
	Size      int64  `json:"size"`
	Deleted   bool   `json:"deleted"`
	Editing   bool   `json:"editing"`
}

func (mgr *DatabaseMgr) CreateDatasetVersion(datasetVersion *DatasetVersion) error {
	return mgr.db.Create(datasetVersion).Error
}

func (mgr *DatabaseMgr) UpdateDatasetVersion(datasetVersion *DatasetVersion) (*DatasetVersion, error) {
	err := mgr.db.Save(datasetVersion).Error
	return datasetVersion, err
}

func (mgr *DatabaseMgr) RecoverDatasetVersion(dsv *DatasetVersion) error {
	sql := fmt.Sprintf("UPDATE dataset_versions SET deleted=? where name='%v' AND workspace='%v' AND version='%v'", dsv.Name, dsv.Workspace, dsv.Version)
	return mgr.db.Exec(sql, false).Error
}

func (mgr *DatabaseMgr) GetDatasetVersion(dsType, workspace, name, version string) (*DatasetVersion, error) {
	var datasetVersion = DatasetVersion{}
	err := mgr.db.First(
		&datasetVersion,
		DatasetVersion{
			Workspace: workspace,
			Name:      name,
			Version:   version,
			Type:      dsType,
		}).Error
	return &datasetVersion, err
}

func (mgr *DatabaseMgr) GetDatasetVersionByID(datasetVersionID uint) (*DatasetVersion, error) {
	var datasetVersion = DatasetVersion{}
	err := mgr.db.First(&datasetVersion, DatasetVersion{ID: datasetVersionID}).Error
	return &datasetVersion, err
}

func (mgr *DatabaseMgr) ListDatasetVersions(filter DatasetVersion) ([]*DatasetVersion, error) {
	var datasetVersions = make([]*DatasetVersion, 0)
	db := mgr.db
	if !filter.Deleted {
		db = db.Where("deleted=?", false)
	}
	err := db.Find(&datasetVersions, filter).Error
	return datasetVersions, err
}

func (mgr *DatabaseMgr) DeleteDatasetVersion(id uint) error {
	return mgr.db.Delete(DatasetVersion{}, DatasetVersion{ID: id}).Error
}

func (mgr *DatabaseMgr) CommitVersion(dsType, workspace, name, version, message string) (*DatasetVersion, error) {
	set := "editing=?"
	values := []interface{}{workspace, name, version, dsType}
	if message != "" {
		set = set + ", message=?"
		values = append([]interface{}{false, message}, values...)
	} else {
		values = append([]interface{}{false}, values...)
	}
	sql := fmt.Sprintf("UPDATE dataset_versions SET %v "+
		"WHERE workspace=? AND name=? AND version=? AND type=?", set)

	err := mgr.db.Exec(sql, values...).Error

	if err != nil {
		return nil, err
	}
	return mgr.GetDatasetVersion(dsType, workspace, name, version)
}

func (mgr *DatabaseMgr) UpdateDatasetVersionSize(dsType, workspace, name, version string) error {
	sql := `UPDATE dataset_versions
	SET
	size = (
		SELECT sum(size) as size from files where files.workspace = dataset_versions.workspace
		AND files.dataset_name = dataset_versions.name
		AND files.version = dataset_versions.version
		AND files.dataset_type = dataset_versions.type
	)
	WHERE dataset_versions.workspace = ? AND
	dataset_versions.name = ? AND
	dataset_versions.version = ? AND
    dataset_versions.type = ?`
	return mgr.db.Exec(sql, workspace, name, version, dsType).Error
}
