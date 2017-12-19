package api

import (
	"net/http"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/gorilla/mux"
	"github.com/kuberlab/pacak/pkg/pacakimpl"
	"github.com/kuberlab/pluk/pkg/utils"
)

type API struct {
	gitInterface pacakimpl.GitInterface
}

func Start() {
	logrus.Info("Starting pluk...")
	api := &API{gitInterface: pacakimpl.NewGitInterface(utils.GitDir(), "/git-local")}

	r := mux.NewRouter()
	r.NotFoundHandler = NotFoundHandler()
	container := restful.NewContainer()
	container.EnableContentEncoding(false)
	ws := new(restful.WebService)
	ws.Path("/v1")
	ws.ApiVersion("v1")
	ws.Produces(restful.MIME_JSON)

	ws.Route(ws.GET("/datasets/{workspace}").To(api.datasets))
	ws.Route(ws.GET("/datasets/{workspace}/{name}/versions").To(api.versions))
	ws.Route(ws.GET("/datasets/{workspace}/{name}/versions/{version}").To(api.getDataset))

	// Check if chunk exists
	ws.Route(ws.GET("/datasets/{hash}").To(api.checkChunk))
	// Save hashed file
	ws.Route(ws.POST("/datasets/{hash}").To(api.saveChunk))

	// Save dataset for version, uploading as an archive.
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
