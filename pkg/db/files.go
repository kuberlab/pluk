package db

type FileMgr interface {
	CreateFile(file *File) error
	UpdateFile(file *File) (*File, error)
	GetFile(fileID uint) (*File, error)
	ListFiles(filter File) ([]*File, error)
}

type File struct {
	BaseModel
	ID             uint    `sql:"AUTO_INCREMENT" gorm:"primary_key"`
	Path           string  `json:"path"`
	RepositoryPath string  `json:"repository_path" gorm:"index:idx_repo_version"`
	Version        string  `json:"version" gorm:"index:idx_repo_version"`
	Chunks         []Chunk `gorm:"many2many:file_chunks;"`
}

func (mgr *DatabaseMgr) CreateFile(file *File) error {
	return mgr.db.Create(file).Error
}

func (mgr *DatabaseMgr) UpdateFile(file *File) (*File, error) {
	if _, err := mgr.GetFile(file.ID); err != nil {
		return nil, err
	}
	err := mgr.db.Save(file).Error
	return file, err
}

func (mgr *DatabaseMgr) GetFile(fileID uint) (*File, error) {
	var file = File{}
	err := mgr.db.First(&file, File{ID: fileID}).Error
	return &file, err
}

func (mgr *DatabaseMgr) ListFiles(filter File) ([]*File, error) {
	var files = make([]*File, 0)
	err := mgr.db.Find(&files, filter).Error
	return files, err
}
