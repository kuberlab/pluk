package api

import (
	"fmt"
	"net/http"
	"os"
	"sync"
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
)

type API struct {
	ds      *datasets.Manager
	mgr     db.DataMgr
	cache   *utils.RequestCache
	fsCache *utils.RequestCache
	client  *http.Client
	hub     *types.Hub

	lock      sync.RWMutex
	saveLocks map[string]*sync.RWMutex
}

func Start() {
	logrus.Info("Starting pluk...")
	utils.PrintEnvInfo()

	port := utils.HttpPort()
	if err := http.ListenAndServe(fmt.Sprintf(":%v", port), GlobalHandler()); err != nil {
		logrus.Error(err)
		os.Exit(1)
	}
}

func GlobalHandler() http.Handler {
	plukio.MasterClient = plukclient.NewInternalMasterClient()
	api := &API{
		cache:     utils.NewRequestCache(),
		fsCache:   utils.NewRequestCache(),
		client:    &http.Client{Timeout: time.Minute},
		ds:        datasets.NewManager(db.DbMgr),
		mgr:       db.DbMgr,
		hub:       types.NewHub(),
		saveLocks: make(map[string]*sync.RWMutex),
	}

	r := mux.NewRouter()
	r.NotFoundHandler = NotFoundHandler()

	// Public API
	apiContainer := NewApiContainer(api, utils.ApiPrefix)
	apiContainer.Filter(api.AuthHook)

	// Internal API
	internalContainer := NewApiContainer(api, utils.InternalPrefix)
	internalContainer.Filter(api.InternalHook)

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
	return WrapLogger(r)
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
	ws.Route(ws.GET("/workspaces/{workspace}/{entityType}/{dataset}").To(api.checkDatasetExists))
	ws.Route(ws.GET("/workspaces/{workspace}/{entityType}/{dataset}/permission").To(api.checkDatasetPermission))
	ws.Route(ws.POST("/workspaces/{workspace}/{entityType}/{dataset}/spec").To(api.postSpec))
	ws.Route(ws.POST("/workspaces/{workspace}/{entityType}/{dataset}/versions/{version}/spec").To(api.postVersionSpec))

	// Items
	ws.Route(ws.GET("/{entityType}").To(api.datasets))
	ws.Route(ws.GET("/{entityType}/{workspace}").To(api.datasets))
	ws.Route(ws.GET("/{entityType}/{workspace}/{name}").To(api.getDataset))
	ws.Route(ws.GET("/{entityType}/{workspace}/{name}/versions/{version}").To(api.downloadDataset))
	ws.Route(ws.POST("/{entityType}/{workspace}/{name}").To(api.createDataset))
	ws.Route(ws.POST("/{entityType}/{workspace}/{name}/fork/{targetWorkspace}").To(api.forkDataset))
	ws.Route(ws.DELETE("/{entityType}/{workspace}/{name}").To(api.deleteDataset))
	ws.Route(ws.GET("/{entityType}/{workspace}/{name}/versions").To(api.versions))
	ws.Route(ws.POST("/{entityType}/{workspace}/{name}/versions/{version}").To(api.createVersion))
	ws.Route(ws.GET("/{entityType}/{workspace}/{name}/versions/{version}/get").To(api.getVersion))
	ws.Route(ws.POST("/{entityType}/{workspace}/{name}/versions/{version}/clone/{targetVersion}").To(api.cloneVersion))
	ws.Route(ws.POST("/{entityType}/{workspace}/{name}/versions/{version}/commit").To(api.commitVersion))
	ws.Route(ws.GET("/{entityType}/{workspace}/{name}/versions/{version}/fs").To(api.getDatasetFS))
	ws.Route(ws.DELETE("/{entityType}/{workspace}/{name}/versions/{version}").To(api.deleteVersion))
	ws.Route(ws.GET("/{entityType}/{workspace}/{name}/versions/{version}/tarsize").To(api.datasetTarSize))
	ws.Route(ws.GET("/{entityType}/{workspace}/{name}/versions/{version}/tree").To(api.fsReadDir))
	ws.Route(ws.GET("/{entityType}/{workspace}/{name}/versions/{version}/tree/{path:*}").To(api.fsReadDir))
	ws.Route(ws.GET("/{entityType}/{workspace}/{name}/versions/{version}/raw/{path:*}").To(api.fsReadFile))
	ws.Route(ws.POST("/{entityType}/{workspace}/{name}/versions/{version}/upload/{path:*}").To(api.uploadDatasetFile))
	ws.Route(ws.DELETE("/{entityType}/{workspace}/{name}/versions/{version}/upload/{path:*}").To(api.deleteDatasetFile))

	// Save file structure for version.
	ws.Route(ws.POST("/{entityType}/{workspace}/{name}/{version}").To(api.saveFS))

	// Check if chunk exists
	ws.Route(ws.GET("/chunks/{hash}").To(api.checkChunk))
	ws.Route(ws.GET("/chunks/{hash}/download").To(api.downloadChunk))
	// Save hashed file chunk
	ws.Route(ws.POST("/chunks/{hash}").To(api.saveChunk))

	// Websocket
	ws.Route(ws.GET("/websocket").To(api.websocket))

	// admin
	ws.Route(ws.GET("/admin/gc").To(api.runGC))
	ws.Route(ws.GET("/admin/clear-chunks").To(api.runClearChunks))

	ws.Filter(setCurrentType)

	container.Add(ws)
	return container
}
