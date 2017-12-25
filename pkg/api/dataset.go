package api

import (
	"fmt"
	"net/http"

	"github.com/emicklei/go-restful"
	"github.com/kuberlab/pluk/pkg/datasets"
)

func (api *API) saveDataset(req *restful.Request, resp *restful.Response) {
	comment := req.HeaderParameter("Comment")
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")

	structure := datasets.FileStructure{}
	err := req.ReadEntity(&structure)
	if err != nil {
		WriteStatusError(resp, http.StatusBadRequest, err)
	}

	dataset := api.ds.NewDataset(workspace, name)
	err = dataset.Save(structure, version, comment)
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.Write([]byte("Ok!\n"))
}

func (api *API) getDataset(req *restful.Request, resp *restful.Response) {
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")

	dataset := api.ds.GetDataset(workspace, name)
	if dataset == nil {
		WriteStatusError(resp, http.StatusNotFound, fmt.Errorf("Dataset '%v' not found", name))
		return
	}
	err := dataset.Download(version, resp)
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.Header().Add("Content-Type", "application/tar+gzip")

	//resp.Header().Add("Content-Disposition", fmt.Sprintf("attachment;filename=%s-%s.%s.tgz;", workspace, name, version))
}

func (api *API) deleteDataset(req *restful.Request, resp *restful.Response) {
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")

	dataset := api.ds.GetDataset(workspace, name)
	if dataset == nil {
		WriteStatusError(resp, http.StatusNotFound, fmt.Errorf("Dataset '%v' not found", name))
		return
	}

	err := dataset.Delete()
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.WriteHeader(http.StatusNoContent)
}

func (api *API) deleteVersion(req *restful.Request, resp *restful.Response) {
	name := req.PathParameter("name")
	version := req.PathParameter("version")
	workspace := req.PathParameter("workspace")

	dataset := api.ds.GetDataset(workspace, name)
	if dataset == nil {
		WriteStatusError(resp, http.StatusNotFound, fmt.Errorf("Dataset '%v' not found", name))
		return
	}

	err := dataset.DeleteVersion(version)
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.WriteHeader(http.StatusNoContent)
}

type CheckChunkResponse struct {
	Hash   string `json:"hash"`
	Exists bool   `json:"exists"`
}

func (api *API) checkChunk(req *restful.Request, resp *restful.Response) {
	hash := req.PathParameter("hash")
	exists := datasets.CheckChunk(hash)

	resp.WriteEntity(CheckChunkResponse{Hash: hash, Exists: exists})
}

func (api *API) saveChunk(req *restful.Request, resp *restful.Response) {
	hash := req.PathParameter("hash")

	if err := datasets.SaveChunk(hash, req.Request.Body); err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.Write([]byte("Ok!\n"))
}

type VersionList struct {
	Versions []string `json:"versions"`
}

func (api *API) versions(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")

	dataset := api.ds.GetDataset(workspace, name)
	if dataset == nil {
		WriteStatusError(resp, http.StatusNotFound, fmt.Errorf("Dataset '%v' not found", name))
		return
	}
	versions, err := dataset.Versions()
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}
	resp.WriteEntity(VersionList{Versions: versions})
}

type DataSetList struct {
	Datasets []*datasets.Dataset `json:"datasets"`
}

func (api *API) datasets(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")

	sets := api.ds.ListDatasets(workspace)
	resp.WriteEntity(DataSetList{Datasets: sets})
}
