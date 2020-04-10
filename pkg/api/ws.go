package api

import (
	"net/http"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/gorilla/websocket"
	libtypes "github.com/kuberlab/lib/pkg/types"
	"github.com/kuberlab/pluk/pkg/types"
)

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func (api API) websocket(req *restful.Request, resp *restful.Response) {
	ws, err := upgrader.Upgrade(resp.ResponseWriter, req.Request, nil)
	if err != nil {
		if _, ok := err.(websocket.HandshakeError); ok {
			WriteStatusError(resp, http.StatusBadRequest, err)
		} else {
			WriteStatusError(resp, http.StatusInternalServerError, err)
		}
		return
	}
	id := req.HeaderParameter("Sec-Websocket-Key")
	ip := strings.Split(req.Request.RemoteAddr, ":")[0]
	wsClient := types.NewWebsocketClient(ws, id, ip)

	api.hub.Register(wsClient)
	api.wsReader(wsClient)
}

func (api *API) wsConnections(req *restful.Request, resp *restful.Response) {
	resp.WriteEntity(api.hub.Connections())
}

func (api *API) lastReceivedMessages(req *restful.Request, resp *restful.Response) {
	response := make(chan []libtypes.Message, 1)
	if api.watcher == nil {
		resp.WriteEntity([]string{})
		return
	}
	api.watcher.getLast <- &Req{Messages: response}

	messages := <-response
	resp.WriteEntity(messages)
}

func (api *API) wsReader(client *types.WebsocketClient) {
	defer client.Ws.Close()
	defer api.hub.Drop(client)
	client.Ws.SetReadLimit(0) // No limit.

	for {
		_, msg, err := client.Ws.ReadMessage()
		if err != nil {
			logrus.Errorf("read: %v", err)
			break
		}
		if string(msg) == "ping" {
			err = client.Ws.WriteMessage(websocket.TextMessage, []byte("pong"))
			if err != nil {
				logrus.Error(err)
				break
			}
			logrus.Infof("Received 'ping' signal from websocket id '%v'", client.ID)
			continue
		}
		//message := libtypes.Message{}
		//err = json.Unmarshal(msg, &message)
		//if err != nil {
		//	if errC, ok := err.(*websocket.CloseError); ok {
		//		if errC.Code == websocket.CloseAbnormalClosure {
		//			break
		//		}
		//	}
		//	logrus.Error(err)
		//	break
		//}
		//
		//switch message.Type {
		//case "chunkData":
		//	chunk := types.ChunkData{}
		//	err = utils.LoadAsJson(message.Content.(map[string]interface{}), &chunk)
		//	if err != nil {
		//		logrus.Error(err)
		//		return
		//	}
		//
		//	err = plukio.SaveChunk(
		//		chunk.Hash,
		//		2,
		//		ioutil.NopCloser(bytes.NewReader(chunk.Data)),
		//		true,
		//	)
		//	if err != nil {
		//		logrus.Error(err)
		//		return
		//	}
		//case "chunkCheck":
		//	check := types.ChunkCheck{}
		//	err = utils.LoadAsJson(message.Content.(map[string]interface{}), &check)
		//	if err != nil {
		//		logrus.Error(err)
		//		return
		//	}
		//	size, exists := plukio.CheckLocalChunk(check.Hash, 2)
		//	check.Exists = exists
		//	check.Size = size
		//	if err := client.WriteMessage(check.Type(), check); err != nil {
		//		return
		//	}
		//}

	}
}
