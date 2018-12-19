package db

import (
	"time"

	"github.com/kuberlab/lib/pkg/types"
)

type FileMgr interface {
	CreateFile(file *File) error
	ForceCreateFile(file *File) error
	UpdateFile(file *File) (*File, error)
	GetFile(workspace, dataset, dsType, path, version string) (*File, error)
	ListFiles(filter File) ([]*File, error)
	DeleteFile(id uint) error
}

type File struct {
	BaseModel
	ID          uint    `sql:"AUTO_INCREMENT" gorm:"primary_key"`
	Path        string  `json:"path" gorm:"unique_index:idx_ws_name_version_path_type"`
	Size        int64   `json:"size"`
	Mode        uint32  `json:"mode"`
	DatasetName string  `json:"dataset_name" gorm:"unique_index:idx_ws_name_version_path_type"`
	DatasetType string  `json:"dataset_type" gorm:"unique_index:idx_ws_name_version_path_type"`
	Workspace   string  `json:"workspace" gorm:"unique_index:idx_ws_name_version_path_type"`
	Version     string  `json:"version" gorm:"unique_index:idx_ws_name_version_path_type"`
	Chunks      []Chunk `gorm:"-"`
}

func (mgr *DatabaseMgr) CreateFile(file *File) error {
	file.CreatedAt = types.NewTime(time.Now())
	file.UpdatedAt = types.NewTime(time.Now())
	return mgr.db.Create(file).Error
}

func (mgr *DatabaseMgr) ForceCreateFile(file *File) error {
	file.CreatedAt = types.NewTime(time.Now())
	file.UpdatedAt = types.NewTime(time.Now())
	if mgr.DBType() == "postgres" {
		tpl := "INSERT INTO files " +
			"(workspace, dataset_type, dataset_name, size, version, mode, path, created_at, updated_at)" +
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (workspace, dataset_type, dataset_name, version, path) DO UPDATE SET " +
			"size=?, mode=?, updated_at=? RETURNING id"
		values := []interface{}{
			file.Workspace, file.DatasetType, file.DatasetName, file.Size,
			file.Version, file.Mode, file.Path, file.CreatedAt, file.UpdatedAt,
			file.Size, file.Mode, file.UpdatedAt,
		}
		var newF = &File{}
		err := mgr.db.Raw(tpl, values...).Scan(newF).Error
		if err != nil {
			return err
		}
		file.ID = newF.ID
		return nil
	} else {
		old, err := mgr.GetFile(file.Workspace, file.DatasetName, file.DatasetType, file.Path, file.Version)
		if err != nil {
			return mgr.db.Create(file).Error
		}
		file.ID = old.ID
		file.Size = old.Size
		file.Mode = old.Mode
		_, err = mgr.UpdateFile(file)
		return err
	}
}

func (mgr *DatabaseMgr) UpdateFile(file *File) (*File, error) {
	file.UpdatedAt = types.NewTime(time.Now())
	err := mgr.db.Save(file).Error
	return file, err
}

func (mgr *DatabaseMgr) GetFile(workspace, dataset, dsType, path, version string) (*File, error) {
	var file = File{}
	err := mgr.db.First(
		&file,
		File{
			Workspace:   workspace,
			DatasetName: dataset,
			Version:     version,
			Path:        path,
			DatasetType: dsType,
		},
	).Error
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
