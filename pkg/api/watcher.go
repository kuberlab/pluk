package api

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/websocket"
	libtypes "github.com/kuberlab/lib/pkg/types"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

const (
	connect = "connect"
	receive = "receive"

	sleepLimit = 10
)

type Watcher struct {
	attempt int
	master  string
	mode    string
	api     *API
	conn    *websocket.Conn
	queue   chan *libtypes.Message
}

func (api *API) StartWatcher() {
	// Start watcher for masters
	if len(utils.Masters()) > 0 {
		watcher := &Watcher{
			master: utils.Masters()[0],
			mode:   "connect",
			queue:  make(chan *libtypes.Message, 10),
			api:    api,
		}
		go watcher.runWatcher()
		go watcher.processQueue()
	}
}

func (w *Watcher) runWatcher() {
	logrus.Info("Starting gc watcher...")
	for {
		switch w.mode {
		case connect:
			// Connect
			w.continuousConnect()
		case receive:
			// Receive messages
			w.continuousReceive()
		}
	}
}

func (w *Watcher) continuousConnect() {
	var toSleep int
	for {
		err := w.connect()
		if err == nil {
			// Now receive
			w.mode = receive
			return
		}

		w.attempt++
		if w.attempt < sleepLimit {
			toSleep = w.attempt
		} else {
			toSleep = sleepLimit
		}
		logrus.Warnf("[Watcher] %v; reconnect in %vs", err, toSleep)
		time.Sleep(time.Second * time.Duration(toSleep))
	}
}

func (w *Watcher) connect() error {
	dialer := &websocket.Dialer{}

	base, err := url.Parse(w.master)
	if err != nil {
		return err
	}
	if len(base.Path) < 2 {
		base.Path = "/pluk/v1"
	}

	var scheme string
	switch base.Scheme {
	case "http":
		scheme = "ws"
	case "https":
		tlsConf := &tls.Config{
			InsecureSkipVerify: true,
		}
		dialer.TLSClientConfig = tlsConf
		scheme = "wss"
	}
	urlStr := fmt.Sprintf("%v://%v/%v", scheme, base.Host, strings.TrimPrefix(base.Path, "/"))
	urlStr = strings.TrimSuffix(urlStr, "/") + "/websocket"

	//urlStr := strings.TrimSuffix(w.master, "/") + "/websocket"
	headers := http.Header{}
	headers.Set("Internal", utils.InternalKey())
	conn, resp, err := dialer.Dial(urlStr, headers)
	if err != nil {
		return fmt.Errorf("Failed to connect: %v", err.Error())
	}
	if resp.StatusCode >= 400 {
		msg, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("Failed to connect: %v", string(msg))
	}

	logrus.Infof("[Watcher] Established connection to %v.", urlStr)
	w.conn = conn
	w.attempt = 0
	return nil
}

func (w *Watcher) continuousReceive() {
	for {
		msg, err := w.receive()
		if err != nil {
			logrus.Errorf("[Watcher] Receive: %v", err)
			// Now connect
			w.mode = connect
			return
		}
		logrus.Debugf("[Watcher] Received message: %v", *msg)
		w.queue <- msg
	}
}

func (w *Watcher) receive() (*libtypes.Message, error) {
	message := &libtypes.Message{}
	err := w.conn.ReadJSON(message)
	if err != nil {
		if errC, ok := err.(*websocket.CloseError); ok {
			if errC.Code == websocket.CloseAbnormalClosure {
				return nil, errC
			}
		}
		return nil, err
	}
	return message, nil
}

func (w *Watcher) processQueue() {
	for m := range w.queue {
		switch m.Type {
		case "dataset":
			ds := &types.Dataset{}
			err := utils.LoadAsJson(m.Content.(map[string]interface{}), ds)
			if err != nil {
				logrus.Error(err)
				break
			}

			// Delete dataset
			logrus.Infof("[Watcher] Delete %v %v/%v", ds.DType, ds.Workspace, ds.Name)
		case "dataset_version":
			dsv := &types.Version{}
			err := utils.LoadAsJson(m.Content.(map[string]interface{}), dsv)
			if err != nil {
				logrus.Error(err)
				break
			}

			// Delete version
			logrus.Infof("[Watcher] Delete %v version %v/%v:%v", dsv.DType, dsv.Workspace, dsv.Name, dsv.Version)
		}
	}
}
