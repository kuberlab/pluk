package api

import (
	"fmt"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	pluk_webdav "github.com/kuberlab/pluk/pkg/webdav"
	"golang.org/x/net/webdav"
)

func (api *API) webdav() http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		version := vars["version"]
		name := vars["name"]
		workspace := vars["workspace"]

		user, pass, _ := req.BasicAuth()
		if user != "" && pass != "" && false {
			resp.Header().Set("WWW-Authenticate", `Basic realm="enter password"`)
			resp.WriteHeader(401)
			resp.Write([]byte("Unauthorized.\n"))
			return
		}

		key := workspace + name + version
		dav := api.cache.GetRaw(key)
		if dav != nil {
			davHandler := dav.(*webdav.Handler)
			davHandler.ServeHTTP(resp, req)
			return
		}

		dataset := api.ds.GetDataset(workspace, name)
		if dataset == nil {
			resp.WriteHeader(http.StatusNotFound)
			resp.Write([]byte(fmt.Sprintf("Dataset %v not found", name)))
			return
		}

		if _, err := dataset.CheckVersion(version); err != nil {
			resp.WriteHeader(http.StatusNotFound)
			resp.Write([]byte(err.Error()))
			return
		}

		// Init file system.
		_, err := dataset.GetFSStructure(version)
		if err != nil {
			resp.WriteHeader(http.StatusNotFound)
			resp.Write([]byte(err.Error()))
		}

		srv := &webdav.Handler{
			Prefix:     fmt.Sprintf("/webdav/%v/%v/%v", workspace, name, version),
			FileSystem: pluk_webdav.NewFS(dataset, version),
			LockSystem: webdav.NewMemLS(),
			Logger: func(r *http.Request, err error) {
				if err != nil {
					logrus.Errorf("WEBDAV ERROR: %v", r, err)
				}
				//logrus.Printf("WEBDAV: %#s, ERROR: %v", r, err)
			},
		}
		api.cache.SetRaw(key, srv)
		srv.ServeHTTP(resp, req)
	}
}
