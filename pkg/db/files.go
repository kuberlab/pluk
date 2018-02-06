package db

type FileMgr interface {
	CreateFile(file *File) error
	UpdateFile(file *File) (*File, error)
	GetFile(workspace, dataset, path, version string) (*File, error)
	ListFiles(filter File) ([]*File, error)
	DeleteFile(id uint) error
}

type File struct {
	BaseModel
	ID          uint    `sql:"AUTO_INCREMENT" gorm:"primary_key"`
	Path        string  `json:"path" gorm:"index:idx_ws_name_version_path"`
	Size        int64   `json:"size"`
	DatasetName string  `json:"dataset_name" gorm:"index:idx_ws_name_version_path"`
	Workspace   string  `json:"workspace" gorm:"index:idx_ws_name_version_path"`
	Version     string  `json:"version" gorm:"index:idx_ws_name_version_path"`
	Chunks      []Chunk `gorm:"-"`
}

func (mgr *DatabaseMgr) CreateFile(file *File) error {
	return mgr.db.Create(file).Error
}

func (mgr *DatabaseMgr) UpdateFile(file *File) (*File, error) {
	err := mgr.db.Save(file).Error
	return file, err
}

func (mgr *DatabaseMgr) GetFile(workspace, dataset, path, version string) (*File, error) {
	var file = File{}
	err := mgr.db.First(&file, File{Workspace: workspace, DatasetName: dataset, Version: version, Path: path}).Error
	return &file, err
}

func (mgr *DatabaseMgr) ListFiles(filter File) ([]*File, error) {
	var files = make([]*File, 0)
	err := mgr.db.Find(&files, filter).Error
	return files, err
}

func (mgr *DatabaseMgr) DeleteFile(id uint) error {
	return mgr.db.Delete(File{}, File{ID: id}).Error
}
