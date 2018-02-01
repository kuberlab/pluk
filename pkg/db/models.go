/*
Package db implements models for tables in database and provides functions
to migrate database state.
*/
package db

import (
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
	).Error
}

// CreateAll is to be used for initial database setup on new deployments or
// adding new tables and columns in existing environments. As for now deletions
// or type convertions of any type should be handled manually.
func CreateAll(db *gorm.DB) error {
	if err := CreateTables(db); err != nil {
		return err
	}

	return nil
}
