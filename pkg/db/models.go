/*
Package db implements models for tables in database and provides functions
to migrate database state.
*/
package db

import (
	"github.com/Sirupsen/logrus"
	"github.com/jinzhu/gorm"
	"github.com/kuberlab/lib/pkg/types"
)

// BaseModel is the basic type for all other models
type BaseModel struct {
	CreatedAt types.Time `json:"-"`
	UpdatedAt types.Time `json:"-"`
}

func NoOp(db *gorm.DB) error {
	return nil
}

func CreateTables(db *gorm.DB) error {
	return db.AutoMigrate(
		&File{},
		&Chunk{},
		&FileChunk{},
		&Dataset{},
		&DatasetVersion{},
	).Error
}

// CreateAll is to be used for initial database setup on new deployments or
// adding new tables and columns in existing environments. As for now deletions
// or type convertions of any type should be handled manually.
func CreateAll(db *gorm.DB) error {
	if err := CreateTables(db); err != nil {
		return err
	}

	if err := db.Debug().Model(&File{}).AddIndex(
		"idx_path",
		"path",
	).Error; err != nil {
		logrus.Error(err)
	}
	if err := db.Debug().Model(&File{}).AddIndex(
		"idx_dataset_workspace",
		"dataset_name", "workspace",
	).Error; err != nil {
		logrus.Error(err)
	}
	if err := db.Debug().Model(&FileChunk{}).AddIndex(
		"idx_file_chunk_index_id",
		"chunk_id", "file_id", "chunk_index",
	).Error; err != nil {
		logrus.Error(err)
	}

	return nil
}
