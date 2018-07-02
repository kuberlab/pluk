package api

import (
	"net/http"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/gorilla/mux"
	"github.com/kuberlab/pluk/pkg/datasets"
	"github.com/kuberlab/pluk/pkg/db"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
	"fmt"
)

type API struct {
	ds      *datasets.Manager
	cache   *utils.RequestCache
	fsCache *utils.RequestCache
	client  *http.Client
	hub     *types.Hub
}

func Start() {
	logrus.Info("Starting pluk...")
	utils.PrintEnvInfo()
	plukio.MasterClient = plukclient.NewInternalMasterClient()
	api := &API{
		cache:   utils.NewRequestCache(),
		fsCache: utils.NewRequestCache(),
		client:  &http.Client{Timeout: time.Minute},
		ds:      datasets.NewManager(db.DbMgr),
		hub:     types.NewHub(),
	}

	r := mux.NewRouter()
	r.NotFoundHandler = NotFoundHandler()

	// Public API
	apiContainer := NewApiContainer(api, utils.ApiPrefix)
	apiContainer.Filter(api.AuthHook)

	// Internal API
	internalContainer := NewApiContainer(api, utils.InternalPrefix)

	r.PathPrefix(utils.ApiPrefix).Handler(apiContainer)
	r.PathPrefix(utils.InternalPrefix).Handler(internalContainer)

	// webdav
	r.PathPrefix("/webdav/{workspace}/{name}/{version}").Handler(api.webdavAuth())

	r.Path("/probe").HandlerFunc(
		func(resp http.ResponseWriter, req *http.Request) {
			resp.Write([]byte("Ok\n"))
		},
	)

	port := utils.HttpPort()

	logrus.Infof("Listen at *:%v", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%v", port), WrapLogger(r)); err != nil {
		logrus.Error(err)
		os.Exit(1)
	}
}

func NewApiContainer(api *API, prefix string) *restful.Container {
	container := restful.NewContainer()
	container.EnableContentEncoding(false)
	ws := new(restful.WebService)
	ws.Path(prefix)
	ws.ApiVersion(utils.ApiVersion)
	ws.Produces(restful.MIME_JSON)

	// to cloud-dealer API
	ws.Route(ws.GET("/workspaces/{workspace}").To(api.checkWorkspace))
	ws.Route(ws.GET("/workspaces/{workspace}/datasets/{dataset}").To(api.checkDataset))

	// Datasets
	ws.Route(ws.GET("/datasets").To(api.datasets))
	ws.Route(ws.GET("/datasets/{workspace}").To(api.datasets))
	ws.Route(ws.DELETE("/datasets/{workspace}/{name}").To(api.deleteDataset))
	ws.Route(ws.GET("/datasets/{workspace}/{name}/versions").To(api.versions))
	ws.Route(ws.GET("/datasets/{workspace}/{name}/versions/{version}").To(api.getDataset))
	ws.Route(ws.GET("/datasets/{workspace}/{name}/versions/{version}/tarsize").To(api.datasetTarSize))
	ws.Route(ws.GET("/datasets/{workspace}/{name}/versions/{version}/fs").To(api.getDatasetFS))
	ws.Route(ws.DELETE("/datasets/{workspace}/{name}/versions/{version}").To(api.deleteVersion))

	// Check if chunk exists
	ws.Route(ws.GET("/chunks/{hash}").To(api.checkChunk))
	ws.Route(ws.GET("/chunks/{hash}/download").To(api.downloadChunk))
	// Save hashed file chunk
	ws.Route(ws.POST("/chunks/{hash}").To(api.saveChunk))

	// Save file structure for version.
	ws.Route(ws.POST("/datasets/{workspace}/{name}/{version}").To(api.saveFS))

	// Websocket
	ws.Route(ws.GET("/websocket").To(api.websocket))

	container.Add(ws)
	return container
}
