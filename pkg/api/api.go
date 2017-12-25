package api

import (
	"net/http"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/gorilla/mux"
	"github.com/kuberlab/pacak/pkg/pacakimpl"
	"github.com/kuberlab/pluk/pkg/datasets"
	"github.com/kuberlab/pluk/pkg/utils"
)

type API struct {
	gitInterface pacakimpl.GitInterface
	ds           *datasets.Manager
	cache        *utils.RequestCache
	client       *http.Client
}

func Start() {
	logrus.Info("Starting pluk...")
	utils.PrintEnvInfo()
	gitIface := pacakimpl.NewGitInterface(utils.GitDir(), utils.GitLocalDir())
	api := &API{
		gitInterface: gitIface,
		cache:        utils.NewRequestCache(),
		client:       &http.Client{Timeout: time.Minute},
		ds:           datasets.NewManager(gitIface),
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
	r.PathPrefix("/webdav/{workspace}/{name}/{version}").Handler(api.webdav())

	r.Path("/probe").HandlerFunc(
		func(resp http.ResponseWriter, req *http.Request) {
			resp.Write([]byte("Ok\n"))
		},
	)
	logrus.Infoln("Listen at *:8082")
	if err := http.ListenAndServe(":8082", WrapLogger(r)); err != nil {
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
	ws.Route(ws.GET("/datasets/{workspace}").To(api.datasets))
	ws.Route(ws.DELETE("/datasets/{workspace}/{name}").To(api.deleteDataset))
	ws.Route(ws.GET("/datasets/{workspace}/{name}/versions").To(api.versions))
	ws.Route(ws.GET("/datasets/{workspace}/{name}/versions/{version}").To(api.getDataset))
	ws.Route(ws.DELETE("/datasets/{workspace}/{name}/versions/{version}").To(api.deleteVersion))

	// Check if chunk exists
	ws.Route(ws.GET("/chunks/{hash}").To(api.checkChunk))
	// Save hashed file chunk
	ws.Route(ws.POST("/chunks/{hash}").To(api.saveChunk))

	// Save file structure for version.
	ws.Route(ws.POST("/datasets/{workspace}/{name}/{version}").To(api.saveDataset))
	container.Add(ws)
	return container
}
