package types

import (
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
)

type Hub struct {
	lock         *sync.RWMutex
	connections  map[*WebsocketClient]bool
	queue        chan Message
	lastMessages *ExpiredMessages
}

const (
	SaveLastMessageLimit = 5
	expireTime           = time.Hour
)

type Message interface {
	Type() string
}

type ExpiredMessages struct {
	messages []Message
	expires  []time.Time
}

func NewExpiredMessages() *ExpiredMessages {
	return &ExpiredMessages{
		expires:  make([]time.Time, 0),
		messages: make([]Message, 0),
	}
}

func (em *ExpiredMessages) AddMessage(message Message) {
	length := len(em.messages)
	expire := time.Now().Add(expireTime)
	if length < SaveLastMessageLimit {
		em.messages = append(em.messages, message)
		em.expires = append(em.expires, expire)
		return
	}

	em.messages = append(em.messages[:SaveLastMessageLimit-1], message)
	em.expires = append(em.expires[:SaveLastMessageLimit-1], expire)
}

func (em *ExpiredMessages) CheckExpire() {
	now := time.Now()
	j := 0
	for i := range em.expires {
		if !now.After(em.expires[i]) {
			// Delete
			em.messages[j] = em.messages[i]
			em.expires[j] = em.expires[i]
			j++
		}
	}
	em.messages = em.messages[:j]
	em.expires = em.expires[:j]
}

func NewHub() *Hub {
	hub := &Hub{
		lock:         &sync.RWMutex{},
		connections:  make(map[*WebsocketClient]bool),
		queue:        make(chan Message, 20),
		lastMessages: NewExpiredMessages(),
	}
	go hub.eventLoop()

	return hub
}

func (h *Hub) Register(client *WebsocketClient) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.lastMessages.CheckExpire()
	h.PushMany(client, h.lastMessages.messages)
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
	h.lastMessages.AddMessage(message)
	h.lastMessages.CheckExpire()
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
