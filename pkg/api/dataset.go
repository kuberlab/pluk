package api

import (
	"net/http"

	"github.com/emicklei/go-restful"
	"github.com/kuberlab/pluk/pkg/dataset"
)

func (api *API) saveDataset(req *restful.Request, resp *restful.Response) {
	comment := req.HeaderParameter("Comment")
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")

	err := dataset.SaveDataset(api.gitInterface, req.Request.Body, workspace, name, version, comment)
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.Write([]byte("Ok!\n"))
}

func (api *API) getDataset(req *restful.Request, resp *restful.Response) {

}

type VersionList struct {
	Versions []string `json:"versions"`
}

func (api *API) versions(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")

	versions, err := dataset.Versions(api.gitInterface, workspace, name)
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}
	resp.WriteEntity(VersionList{Versions: versions})
}

type DataSetList struct {
	Datasets []string `json:"datasets"`
}

func (api *API) datasets(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")

	sets, err := dataset.Datasets(workspace)
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}
	resp.WriteEntity(DataSetList{Datasets: sets})
}
