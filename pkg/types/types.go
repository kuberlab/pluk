package types

import (
	"os"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/websocket"
	"github.com/kuberlab/pluk/pkg/utils"
)

type Workspace struct {
	Name        string
	DisplayName string
	Type        string
	Can         []string
}

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
