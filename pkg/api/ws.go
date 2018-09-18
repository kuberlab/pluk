package api

import (
	"bytes"
	"io/ioutil"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/gorilla/websocket"
	libtypes "github.com/kuberlab/lib/pkg/types"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
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
	wsClient := types.NewWebsocketClient(ws, id)

	api.hub.Register(wsClient)
	api.wsReader(wsClient)
}

func (api *API) wsReader(client *types.WebsocketClient) {
	defer client.Ws.Close()
	defer api.hub.Drop(client)
	client.Ws.SetReadLimit(0) // No limit.

	for {
		message := libtypes.Message{}
		err := client.Ws.ReadJSON(&message)
		if err != nil {
			if errC, ok := err.(*websocket.CloseError); ok {
				if errC.Code == websocket.CloseAbnormalClosure {
					break
				}
			}
			logrus.Error(err)
			break
		}

		switch message.Type {
		case "chunkData":
			chunk := types.ChunkData{}
			err = utils.LoadAsJson(message.Content.(map[string]interface{}), &chunk)
			if err != nil {
				logrus.Error(err)
				return
			}

			err = plukio.SaveChunk(
				chunk.Hash,
				ioutil.NopCloser(bytes.NewReader(chunk.Data)),
				true,
			)
			if err != nil {
				logrus.Error(err)
				return
			}
		case "chunkCheck":
			check := types.ChunkCheck{}
			err = utils.LoadAsJson(message.Content.(map[string]interface{}), &check)
			if err != nil {
				logrus.Error(err)
				return
			}
			size, exists := plukio.CheckLocalChunk(check.Hash)
			check.Exists = exists
			check.Size = size
			if err := client.WriteMessage(check.Type(), check); err != nil {
				return
			}
		}

	}
}
