package api

import (
	"fmt"
	"io"
	"net/http"

	"github.com/emicklei/go-restful"
)

func (api *API) fsReadDir(req *restful.Request, resp *restful.Response) {
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
	path := req.PathParameter("path")
	master := api.masterClient(req)

	dataset := api.ds.GetDataset(workspace, name, master)
	if dataset == nil {
		WriteStatusError(resp, http.StatusNotFound, fmt.Errorf("Dataset '%v' not found", name))
		return
	}
	fs, err := api.getFS(dataset, version)
	if err != nil {
		WriteError(resp, err)
		return
	}

	result, err := fs.Readdir(path, 0)
	if err != nil {
		WriteStatusError(resp, http.StatusNotFound, err)
		return
	}

	resp.WriteEntity(result)
}

func (api *API) fsReadFile(req *restful.Request, resp *restful.Response) {
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
	path := req.PathParameter("path")
	master := api.masterClient(req)

	dataset := api.ds.GetDataset(workspace, name, master)
	if dataset == nil {
		WriteStatusError(resp, http.StatusNotFound, fmt.Errorf("Dataset '%v' not found", name))
		return
	}
	fs, err := api.getFS(dataset, version)
	if err != nil {
		WriteError(resp, err)
		return
	}

	file := fs.GetFile(path)
	if file == nil || file.Fstat.IsDir() {
		WriteErrorString(resp, http.StatusNotFound, fmt.Sprintf("No such file: %v", path))
		return
	}

	resp.WriteHeader(http.StatusOK)
	resp.AddHeader("Content-Length", fmt.Sprintf("%v", file.Size))
	io.Copy(resp, file)
}
