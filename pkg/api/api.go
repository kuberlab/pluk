package api

import (
	"net/http"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/gorilla/mux"
)

type API struct{}

func Start() {
	logrus.Info("Starting pluk...")
	//api := API{}

	r := mux.NewRouter()
	r.NotFoundHandler = NotFoundHandler()
	container := restful.NewContainer()
	container.EnableContentEncoding(false)
	ws := new(restful.WebService)
	ws.Path("/v1")
	ws.ApiVersion("v1")
	ws.Produces(restful.MIME_JSON)

	//ws.Route(ws.GET("/apps/{app}").To(api.app))

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
