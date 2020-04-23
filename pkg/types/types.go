package types

import (
	"os"
	"sync"
	"time"

	"github.com/Masterminds/semver"
	"github.com/gorilla/websocket"
	"github.com/kuberlab/lib/pkg/dealerclient"
	"github.com/kuberlab/lib/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

const (
	ChunkVersion byte = 2
)

type Workspace dealerclient.Workspace

type DataSetList struct {
	Items []Dataset `json:"items"`
}

func (d DataSetList) Len() int {
	return len(d.Items)
}

func (d DataSetList) Less(i, j int) bool {
	return d.Items[i].Name < d.Items[j].Name
}

func (d DataSetList) Swap(i, j int) {
	d.Items[i], d.Items[j] = d.Items[j], d.Items[i]
}

type Dataset struct {
	Workspace string `json:"workspace"`
	Name      string `json:"name"`
	DType     string `json:"type"`
}

func (d *Dataset) Type() string {
	return "dataset"
}

type VersionList struct {
	Versions []Version `json:"versions"`
}

type VersionArr []Version

func (vl VersionArr) Len() int {
	return len(vl)
}

func (vl VersionArr) Less(i, j int) bool {
	v1 := vl[i].Version
	v2 := vl[j].Version
	sv1, err := semver.NewVersion(v1)
	if err != nil {
		return true
	}

	sv2, err := semver.NewVersion(v2)
	if err != nil {
		return false
	}
	return sv1.LessThan(sv2)
}

func (vl VersionArr) Swap(i, j int) {
	vl[i], vl[j] = vl[j], vl[i]
}

type Version struct {
	Version   string     `json:"version"`
	CreatedAt types.Time `json:"created_at"`
	UpdatedAt types.Time `json:"updated_at"`
	SizeBytes int64      `json:"size_bytes"`
	FileCount int64      `json:"file_count"`
	Message   string     `json:"message,omitempty"`
	Workspace string     `json:"workspace"`
	Name      string     `json:"name"`
	DType     string     `json:"type,omitempty"`
	Editing   bool       `json:"editing"`
}

func (dv *Version) Type() string {
	return "dataset_version"
}

type SaveOpts struct {
	Comment string
	Create  bool
	Publish bool
	Editing bool
}

type FileStructure struct {
	Files []*HashedFile `json:"files"`
}

type HashedFile struct {
	Path     string      `json:"path"`
	Size     int64       `json:"size"`
	Hashes   []Hash      `json:"hashes"`
	Mode     os.FileMode `json:"mode"`
	ModeTime time.Time   `json:"mode_time"`
}

type Hash struct {
	Hash    string `json:"hash"`
	Size    int64  `json:"size"`
	Version byte   `json:"version"`
}

type ChunkCheck struct {
	Hash   string `json:"hash"`
	Size   int64  `json:"size"`
	Exists bool   `json:"exists"`
}

func (c *ChunkCheck) Type() string {
	return "chunkCheck"
}

type ChunkData struct {
	Data []byte `json:"data"`
	Hash string `json:"hash"`
}

func (c *ChunkData) Type() string {
	return "chunkData"
}

type WebsocketClient struct {
	lock        *sync.RWMutex   `json:"-"`
	Ws          *websocket.Conn `json:"-"`
	ID          string          `json:"id"`
	IP          string          `json:"ip"`
	ConnectedAt types.Time      `json:"connected_at"`
	Closed      bool
}

func NewWebsocketClient(ws *websocket.Conn, id, ip string) *WebsocketClient {
	return &WebsocketClient{
		Ws:          ws,
		ID:          id,
		IP:          ip,
		lock:        &sync.RWMutex{},
		ConnectedAt: types.NewTime(time.Now()),
	}
}

func (c *WebsocketClient) closeHandler() {

}

func (c *WebsocketClient) WriteMessage(sType string, content interface{}) error {
	// Prevent concurrent socket writes.
	c.lock.Lock()
	defer c.lock.Unlock()
	return utils.WriteMessage(c.Ws, sType, c.ID, content)
}
