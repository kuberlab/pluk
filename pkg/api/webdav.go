package api

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/kuberlab/pluk/pkg/utils"
	pluk_webdav "github.com/kuberlab/pluk/pkg/webdav"
	"golang.org/x/net/webdav"
)

func (api *API) webdavAuth() http.HandlerFunc {
	writeUnauthorized := func(resp http.ResponseWriter) {
		resp.Header().Set("WWW-Authenticate", `Basic realm="enter password"`)
		resp.WriteHeader(http.StatusUnauthorized)
		resp.Write([]byte("Unauthorized.\n"))
	}

	return func(resp http.ResponseWriter, req *http.Request) {
		user, pass, _ := req.BasicAuth()
		key := user + pass

		authURL := utils.AuthValidationURL()
		if authURL == "" && !utils.HasMasters() {
			// Skip.
			api.webdav().ServeHTTP(resp, req)
			return
		}

		if api.cache.Get(key) {
			api.webdav().ServeHTTP(resp, req)
			return
		}

		vars := mux.Vars(req)
		workspace := vars["workspace"]

		if authURL != "" {
			u, err := url.Parse(authURL)
			if err != nil {
				http.Error(resp, err.Error(), http.StatusInternalServerError)
				return
			}
			workspaceValidationUrl := fmt.Sprintf("%v://%v/api/v0.2/workspace/%v/secret/%v", u.Scheme, u.Host, workspace, pass)
			request, _ := http.NewRequest("GET", workspaceValidationUrl, nil)

			logrus.Debugf("GET %v://%v/[redacted]", request.URL.Scheme, request.URL.Host)
			r, err := api.client.Do(request)
			if err != nil {
				http.Error(resp, err.Error(), http.StatusInternalServerError)
				return
			}
			logrus.Debugf("Got %v", r.StatusCode)
			if r.StatusCode >= 400 {
				logrus.Error(fmt.Sprintf("Invalid auth to %v://%v/[redacted]", request.URL.Scheme, request.URL.Host))
				writeUnauthorized(resp)
				return
			}
		} else if utils.HasMasters() {
			yes, err := plukio.MasterClient.WebdavAuth(user, pass, req.URL.Path)
			if err != nil || !yes {
				logrus.Errorf("Invalid auth to master: %v", err)
				writeUnauthorized(resp)
				return
			}
		}

		api.cache.Set(key, true)

		api.webdav().ServeHTTP(resp, req)
	}
}

func (api *API) webdav() http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		version := vars["version"]
		name := vars["name"]
		workspace := vars["workspace"]

		key := workspace + name + version
		dav := api.cache.GetRaw(key)
		if dav != nil {
			davHandler := dav.(*webdav.Handler)
			davHandler.ServeHTTP(resp, req)
			return
		}

		dataset := api.ds.GetDataset("dataset", workspace, name, plukclient.NewInternalMasterClient())
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
		fs, err := api.getFS(dataset, version)
		if err != nil {
			resp.WriteHeader(http.StatusNotFound)
			resp.Write([]byte(err.Error()))
			return
		}
		dataset.FS = fs

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
