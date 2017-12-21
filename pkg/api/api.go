package api

import (
	"net/http"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/gorilla/mux"
	"github.com/kuberlab/pacak/pkg/pacakimpl"
	"github.com/kuberlab/pluk/pkg/utils"
)

type API struct {
	gitInterface pacakimpl.GitInterface
	cache        *utils.RequestCache
	client       *http.Client
}

func Start() {
	logrus.Info("Starting pluk...")
	api := &API{
		gitInterface: pacakimpl.NewGitInterface(utils.GitDir(), "/git-local"),
		cache:        utils.NewRequestCache(),
		client:       &http.Client{Timeout: time.Minute},
	}

	r := mux.NewRouter()
	r.NotFoundHandler = NotFoundHandler()
	container := restful.NewContainer()
	container.EnableContentEncoding(false)
	ws := new(restful.WebService)
	ws.Path("/v1")
	ws.ApiVersion("v1")
	ws.Produces(restful.MIME_JSON)

	ws.Filter(api.AuthHook)
	ws.Route(ws.GET("/datasets/{workspace}").To(api.datasets))
	ws.Route(ws.GET("/datasets/{workspace}/{name}/versions").To(api.versions))
	ws.Route(ws.GET("/datasets/{workspace}/{name}/versions/{version}").To(api.getDataset))

	// Check if chunk exists
	ws.Route(ws.GET("/chunks/{hash}").To(api.checkChunk))
	// Save hashed file chunk
	ws.Route(ws.POST("/chunks/{hash}").To(api.saveChunk))

	// Save file structure for version.
	ws.Route(ws.POST("/datasets/{workspace}/{name}/{version}").To(api.saveDataset))

	container.Add(ws)
	r.PathPrefix("/v1/").Handler(container)
	r.Path("/probe").HandlerFunc(
		func(resp http.ResponseWriter, req *http.Request) {
			resp.Write([]byte("Ok\n"))
		},
	)
	logrus.Infoln("Listen in *:8082")
	if err := http.ListenAndServe(":8082", WrapLogger(r)); err != nil {
		logrus.Error(err)
		os.Exit(1)
	}
}
