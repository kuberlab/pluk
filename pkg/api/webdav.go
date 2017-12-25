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

		dataset := api.ds.GetDataset(workspace, name)
		if dataset == nil {
			resp.WriteHeader(http.StatusNotFound)
			//WriteStatusError(resp, http.StatusNotFound, fmt.Errorf("Dataset '%v' not found", name))
			return
		}

		if _, err := dataset.CheckoutVersion(version); err != nil {
			resp.WriteHeader(http.StatusNotFound)
			resp.Write([]byte(err.Error()))
			return
		}
		srv := &webdav.Handler{
			Prefix:     fmt.Sprintf("/webdav/%v/%v/%v", workspace, name, version),
			FileSystem: &pluk_webdav.FS{Dataset: dataset},
			LockSystem: webdav.NewMemLS(),
			Logger: func(r *http.Request, err error) {
				if err != nil {
					logrus.Errorf("WEBDAV ERROR: %v", r, err)
				}
				//logrus.Printf("WEBDAV: %#s, ERROR: %v", r, err)
			},
		}
		srv.ServeHTTP(resp, req)
	}
}
