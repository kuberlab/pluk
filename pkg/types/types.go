package types

import (
	"os"
	"sync"
	"time"

	"github.com/Masterminds/semver"
	"github.com/Sirupsen/logrus"
	"github.com/gorilla/websocket"
	"github.com/kuberlab/lib/pkg/dealerclient"
	"github.com/kuberlab/lib/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
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
	Type      string `json:"type"`
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
	Type      string     `json:"type"`
	Editing   bool       `json:"editing"`
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
	Hash string `json:"hash"`
	Size int64  `json:"size"`
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
	lock *sync.RWMutex
	Ws   *websocket.Conn
	ID   string
}

func NewWebsocketClient(ws *websocket.Conn, id string) *WebsocketClient {
	return &WebsocketClient{Ws: ws, ID: id, lock: &sync.RWMutex{}}
}

func (c *WebsocketClient) WriteMessage(sType string, content interface{}) error {
	// Prevent concurrent socket writes.
	c.lock.Lock()
	defer c.lock.Unlock()
	return utils.WriteMessage(c.Ws, sType, c.ID, content)
}

type Hub struct {
	lock        *sync.RWMutex
	connections map[*WebsocketClient]bool
}

type Message interface {
	Type() string
}

func NewHub() *Hub {
	return &Hub{
		lock:        &sync.RWMutex{},
		connections: make(map[*WebsocketClient]bool),
	}
}

func (h *Hub) Register(client *WebsocketClient) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.connections[client] = true
}

func (h *Hub) Drop(client *WebsocketClient) {
	h.lock.Lock()
	defer h.lock.Unlock()
	if _, ok := h.connections[client]; ok {
		delete(h.connections, client)
	}
}

func (h *Hub) PushMany(client *WebsocketClient, statuses []Message) {
	for _, s := range statuses {
		client.WriteMessage(s.Type(), s)
	}
}

func (h *Hub) Push(message Message) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	for client := range h.connections {
		err := client.WriteMessage(message.Type(), message)
		if err != nil {
			logrus.Error(err)
		}
	}
}
