package db

type DatasetVersionVersionMgr interface {
	CreateDatasetVersion(datasetVersionVersion *DatasetVersion) error
	UpdateDatasetVersion(datasetVersion *DatasetVersion) (*DatasetVersion, error)
	GetDatasetVersion(workspace, name, version string) (*DatasetVersion, error)
	GetDatasetVersionByID(datasetVersionID uint) (*DatasetVersion, error)
	ListDatasetVersions(filter DatasetVersion) ([]*DatasetVersion, error)
	DeleteDatasetVersion(id uint) error
}

type DatasetVersion struct {
	BaseModel
	ID        uint   `sql:"AUTO_INCREMENT" gorm:"primary_key"`
	Workspace string `json:"workspace" gorm:"index:idx_workspace_name"`
	Name      string `json:"name" gorm:"index:idx_workspace_name"`
	Version   string `json:"version" gorm:"index:idx_version"`
	Deleted   bool   `json:"deleted"`
}

func (mgr *DatabaseMgr) CreateDatasetVersion(datasetVersion *DatasetVersion) error {
	return mgr.db.Create(datasetVersion).Error
}

func (mgr *DatabaseMgr) UpdateDatasetVersion(datasetVersion *DatasetVersion) (*DatasetVersion, error) {
	err := mgr.db.Save(datasetVersion).Error
	return datasetVersion, err
}

func (mgr *DatabaseMgr) GetDatasetVersion(workspace, name, version string) (*DatasetVersion, error) {
	var datasetVersion = DatasetVersion{}
	err := mgr.db.First(&datasetVersion, DatasetVersion{Workspace: workspace, Name: name, Version: version}).Error
	return &datasetVersion, err
}

func (mgr *DatabaseMgr) GetDatasetVersionByID(datasetVersionID uint) (*DatasetVersion, error) {
	var datasetVersion = DatasetVersion{}
	err := mgr.db.First(&datasetVersion, DatasetVersion{ID: datasetVersionID}).Error
	return &datasetVersion, err
}

func (mgr *DatabaseMgr) ListDatasetVersions(filter DatasetVersion) ([]*DatasetVersion, error) {
	var datasetVersions = make([]*DatasetVersion, 0)
	err := mgr.db.Find(&datasetVersions, filter).Error
	return datasetVersions, err
}

func (mgr *DatabaseMgr) DeleteDatasetVersion(id uint) error {
	return mgr.db.Delete(DatasetVersion{}, DatasetVersion{ID: id}).Error
}
