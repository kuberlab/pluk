package types

import (
	"sync"

	"github.com/Sirupsen/logrus"
)

type Hub struct {
	lock        *sync.RWMutex
	connections map[*WebsocketClient]bool
	queue       chan Message
}

type Message interface {
	Type() string
}

func NewHub() *Hub {
	hub := &Hub{
		lock:        &sync.RWMutex{},
		connections: make(map[*WebsocketClient]bool),
		queue:       make(chan Message, 20),
	}
	go hub.eventLoop()

	return hub
}

func (h *Hub) Register(client *WebsocketClient) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.connections[client] = true
}

func (h *Hub) Connections() []*WebsocketClient {
	h.lock.RLock()
	defer h.lock.RUnlock()
	res := make([]*WebsocketClient, 0)
	for client := range h.connections {
		res = append(res, client)
	}
	return res
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
		if err := client.WriteMessage(s.Type(), s); err != nil {
			logrus.Error(err)
		}
	}
}

func (h *Hub) eventLoop() {
	// Read buffered channel for messages indefinitely
	for message := range h.queue {
		h.pushMessage(message)
	}
}

func (h *Hub) Push(message Message) {
	h.queue <- message
}

func (h *Hub) pushMessage(status Message) {
	// Lock using Lock instead of RLock to prevent parallel pushing.
	h.lock.Lock()
	defer h.lock.Unlock()
	for client := range h.connections {
		err := client.WriteMessage(status.Type(), status)
		if err != nil {
			logrus.Error(err)
		}
	}
}
