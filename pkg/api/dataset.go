package api

import (
	"fmt"
	"io"
	"net/http"

	"github.com/emicklei/go-restful"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
)

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

func (api *API) getDatasetFS(req *restful.Request, resp *restful.Response) {
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")

	dataset := api.ds.GetDataset(workspace, name)
	if dataset == nil {
		WriteStatusError(resp, http.StatusNotFound, fmt.Errorf("Dataset '%v' not found", name))
		return
	}
	infos, err := dataset.GetFSStructure(version)
	if err != nil {
		WriteStatusError(resp, http.StatusNotFound, err)
		return
	}

	resp.WriteEntity(infos)
	//resp.Header().Add("Content-Type", "application/tar+gzip")
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

func (api *API) checkChunk(req *restful.Request, resp *restful.Response) {
	hash := req.PathParameter("hash")
	exists := plukio.CheckChunk(hash)

	resp.WriteEntity(types.CheckChunkResponse{Hash: hash, Exists: exists})
}

func (api *API) downloadChunk(req *restful.Request, resp *restful.Response) {
	hash := req.PathParameter("hash")
	file, err := plukio.GetChunk(hash)
	if err != nil {
		WriteStatusError(resp, http.StatusNotFound, err)
	}

	io.Copy(resp, file)
}

func (api *API) saveChunk(req *restful.Request, resp *restful.Response) {
	hash := req.PathParameter("hash")

	if err := plukio.SaveChunk(hash, req.Request.Body, true); err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.Write([]byte("Ok!\n"))
}

func (api *API) saveFS(req *restful.Request, resp *restful.Response) {
	comment := req.HeaderParameter("Comment")
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")

	structure := types.FileStructure{}
	err := req.ReadEntity(&structure)
	if err != nil {
		WriteStatusError(resp, http.StatusBadRequest, err)
	}

	dataset := api.ds.NewDataset(workspace, name)
	if err = dataset.InitRepo(true); err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}
	err = dataset.Save(structure, version, comment)
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.Write([]byte("Ok!\n"))
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
	resp.WriteEntity(types.VersionList{Versions: versions})
}

func (api *API) datasets(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")

	sets := api.ds.ListDatasets(workspace)
	ds := types.DataSetList{}
	for _, d := range sets {
		ds.Datasets = append(ds.Datasets, &d.Dataset)
	}
	if len(ds.Datasets) == 0 {
		ds.Datasets = make([]*types.Dataset, 0)
	}
	resp.WriteEntity(ds)
}
