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

	structure := dataset.FileStructure{}
	err := req.ReadEntity(&structure)
	if err != nil {
		WriteStatusError(resp, http.StatusBadRequest, err)
	}

	err = dataset.SaveDataset(api.gitInterface, structure, workspace, name, version, comment)
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

	err := dataset.GetDataset(api.gitInterface, workspace, name, version, resp)
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.Header().Add("Content-Type", "application/tar+gzip")

	//resp.Header().Add("Content-Disposition", fmt.Sprintf("attachment;filename=%s-%s.%s.tgz;", workspace, name, version))
	//_, err = io.Copy(resp, data)
	//if err != nil {
	//	WriteStatusError(resp, http.StatusInternalServerError, err)
	//	return
	//}
}

type CheckChunkResponse struct {
	Hash   string `json:"hash"`
	Exists bool   `json:"exists"`
}

func (api *API) checkChunk(req *restful.Request, resp *restful.Response) {
	hash := req.PathParameter("hash")
	exists := dataset.CheckChunk(hash)

	resp.WriteEntity(CheckChunkResponse{Hash: hash, Exists: exists})
}

func (api *API) saveChunk(req *restful.Request, resp *restful.Response) {
	hash := req.PathParameter("hash")

	if err := dataset.SaveChunk(hash, req.Request.Body); err != nil {
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
