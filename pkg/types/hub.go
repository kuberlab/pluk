package types

import (
	"sync"

	"github.com/Sirupsen/logrus"
)

type Hub struct {
	lock         *sync.RWMutex
	connections  map[*WebsocketClient]bool
	queue        chan Message
	lastMessages []Message
}

const (
	SaveLastMessageLimit = 5
)

type Message interface {
	Type() string
}

func NewHub() *Hub {
	hub := &Hub{
		lock:         &sync.RWMutex{},
		connections:  make(map[*WebsocketClient]bool),
		queue:        make(chan Message, 20),
		lastMessages: make([]Message, 0),
	}
	go hub.eventLoop()

	return hub
}

func (h *Hub) Register(client *WebsocketClient) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.PushMany(client, h.lastMessages)
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

func (h *Hub) saveLastMessage(message Message) {
	length := len(h.lastMessages)
	if length < SaveLastMessageLimit {
		h.lastMessages = append(h.lastMessages, message)
		return
	}

	h.lastMessages = append(h.lastMessages[:SaveLastMessageLimit-1], message)
}

func (h *Hub) pushMessage(message Message) {
	// Lock using Lock instead of RLock to prevent parallel pushing.
	h.lock.Lock()
	defer h.lock.Unlock()
	h.saveLastMessage(message)
	for client := range h.connections {
		err := client.WriteMessage(message.Type(), message)
		if err != nil {
			logrus.Error(err)
		}
	}
}
