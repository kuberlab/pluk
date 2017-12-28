package types

import (
	"os"
	"time"
)

type DataSetList struct {
	Datasets []*Dataset `json:"datasets"`
}

type Dataset struct {
	Workspace string `json:"workspace"`
	Name      string `json:"name"`
}

type VersionList struct {
	Versions []string `json:"versions"`
}

type CheckChunkResponse struct {
	Hash   string `json:"hash"`
	Exists bool   `json:"exists"`
}

type FileStructure struct {
	Files []*HashedFile `json:"files"`
}

type HashedFile struct {
	Path     string      `json:"path"`
	Size     uint64      `json:"size"`
	Hashes   []string    `json:"hashes"`
	Mode     os.FileMode `json:"mode"`
	ModeTime time.Time   `json:"mode_time"`
}
