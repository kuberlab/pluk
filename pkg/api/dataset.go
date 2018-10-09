package api

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/dealerclient"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/datasets"
	"github.com/kuberlab/pluk/pkg/db"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

func (api *API) masterClient(req *restful.Request) plukio.PlukClient {
	masterRaw := req.Attribute("masterclient")
	if masterRaw == nil {
		return nil
	}
	if master, ok := masterRaw.(plukio.PlukClient); ok {
		return master
	}
	return nil
}

func (api *API) getDataset(req *restful.Request, resp *restful.Response) {
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
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
	dataset.FS = fs

	sz, err := dataset.TarSize()
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.Header().Add("Content-Type", "application/tar")
	resp.Header().Add("Content-Length", fmt.Sprintf("%v", sz))

	err = dataset.Download(resp)
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	//resp.Header().Add("Content-Disposition", fmt.Sprintf("attachment;filename=%s-%s.%s.tgz;", workspace, name, version))
}

func (api *API) datasetTarSize(req *restful.Request, resp *restful.Response) {
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
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
	dataset.FS = fs

	sz, err := dataset.TarSize()
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.Write([]byte(fmt.Sprintf("%v\n", sz)))
}

func (api *API) getDatasetFS(req *restful.Request, resp *restful.Response) {
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
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

	resp.WriteEntity(fs)
	//resp.Header().Add("Content-Type", "application/tar+gzip")
	//resp.Header().Add("Content-Disposition", fmt.Sprintf("attachment;filename=%s-%s.%s.tgz;", workspace, name, version))
}

func (api *API) deleteDataset(req *restful.Request, resp *restful.Response) {
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
	forceRaw := req.QueryParameter("force")
	force, _ := strconv.ParseBool(forceRaw)
	master := api.masterClient(req)

	err := api.ds.DeleteDataset(workspace, name, master, force)
	if err != nil {
		WriteError(resp, err)
		return
	}

	if utils.AuthValidationURL() != "" {
		dealer, err := api.dealerClient(req)
		if err != nil {
			WriteErrorString(resp, http.StatusBadRequest, fmt.Sprintf("Can not delete dataset '%v' from API: %v", name, err))
			return
		}
		// Skip error
		err = dealer.DeleteDataset(workspace, name)
		if err != nil {
			logrus.Error(err)
		}
	}

	resp.WriteHeader(http.StatusNoContent)
}

func (api *API) deleteVersion(req *restful.Request, resp *restful.Response) {
	name := req.PathParameter("name")
	version := req.PathParameter("version")
	workspace := req.PathParameter("workspace")
	master := api.masterClient(req)

	dataset := api.ds.GetDataset(workspace, name, master)
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

	chunkCheck, err := plukio.CheckChunk(hash)
	if err != nil {
		WriteError(resp, err)
		return
	}
	resp.WriteEntity(chunkCheck)
}

func (api *API) downloadChunk(req *restful.Request, resp *restful.Response) {
	hash := req.PathParameter("hash")
	file, err := plukio.GetChunk(hash)
	if err != nil {
		WriteStatusError(resp, http.StatusNotFound, err)
		return
	}

	resp.WriteHeader(http.StatusOK)
	io.Copy(resp, file)
	err = file.Close()
	if err != nil {
		logrus.Error(err)
	}
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
	createRaw := req.QueryParameter("create")
	create, _ := strconv.ParseBool(createRaw)
	publishRaw := req.QueryParameter("publish")
	publish, _ := strconv.ParseBool(publishRaw)
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
	master := api.masterClient(req)

	structure := types.FileStructure{}
	err := req.ReadEntity(&structure)
	if err != nil {
		WriteStatusError(resp, http.StatusBadRequest, err)
		return
	}

	if err = utils.CheckVersion(version); err != nil {
		WriteStatusError(resp, http.StatusBadRequest, err)
		return
	}

	dataset, err := api.ds.NewDataset(workspace, name, master)
	if err != nil {
		WriteError(resp, err)
		return
	}
	err = dataset.Save(structure, version, comment, create, publish, true)
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	if create {
		if err = api.createDatasetOnDealer(req, workspace, name, publish); err != nil {
			WriteStatusError(resp, http.StatusInternalServerError, err)
			return
		}
	}

	resp.Write([]byte("Ok!\n"))
}

func (api *API) versions(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")
	master := api.masterClient(req)

	dataset := api.ds.GetDataset(workspace, name, master)
	if dataset == nil {
		WriteStatusError(resp, http.StatusNotFound, fmt.Errorf("Dataset '%v' not found", name))
		return
	}
	versions, err := dataset.Versions()
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	// Cache last 3 versions.
	onlyVersions := make([]string, 0)
	for _, v := range versions {
		onlyVersions = append(onlyVersions, v.Version)
	}
	go api.cacheFS(dataset, utils.GetFirstN(onlyVersions, 3))
	resp.WriteEntity(types.VersionList{Versions: versions})
}

func (api *API) createDataset(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")
	master := api.masterClient(req)

	dataset := api.ds.GetDataset(workspace, name, master)
	if dataset != nil {
		WriteStatusError(resp, http.StatusConflict, fmt.Errorf("Dataset '%v' already exists", name))
		return
	}

	ds := &db.Dataset{Workspace: workspace, Name: name}
	if err := api.mgr.CreateDataset(ds); err != nil {
		WriteError(resp, err)
		return
	}
	resp.WriteHeaderAndEntity(http.StatusCreated, ds)
}

func (api *API) cloneVersion(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")
	version := req.PathParameter("version")
	targetVersion := req.PathParameter("targetVersion")
	master := api.masterClient(req)

	dataset := api.ds.GetDataset(workspace, name, master)
	if dataset == nil {
		WriteStatusError(resp, http.StatusNotFound, fmt.Errorf("Dataset '%v' not found", name))
		return
	}

	if err := utils.CheckVersion(targetVersion); err != nil {
		WriteStatusError(resp, http.StatusBadRequest, err)
		return
	}

	dsv, err := dataset.CloneVersion(version, targetVersion)
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.WriteEntity(dsv)
}

func (api *API) createVersion(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")
	version := req.PathParameter("version")
	master := api.masterClient(req)

	dataset := api.ds.GetDataset(workspace, name, master)
	if dataset == nil {
		// Create
		var err error
		dataset, err = api.ds.NewDataset(workspace, name, master)
		if err != nil {
			WriteError(resp, err)
			return
		}
	}

	if err := utils.CheckVersion(version); err != nil {
		WriteStatusError(resp, http.StatusBadRequest, err)
		return
	}

	versions, err := dataset.Versions()
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	for _, v := range versions {
		if v.Version == version {
			WriteStatusError(
				resp,
				http.StatusConflict,
				fmt.Errorf("Version %v for dataset %v/%v already exists", version, workspace, name),
			)
		}
	}

	dsv := &db.DatasetVersion{
		Version:   version,
		Name:      name,
		Workspace: workspace,
		Editing:   true,
	}
	if err := api.mgr.CreateDatasetVersion(dsv); err != nil {
		WriteError(resp, err)
		return
	}

	resp.WriteHeaderAndEntity(http.StatusCreated, dsv)
}

func (api *API) commitVersion(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")
	version := req.PathParameter("version")
	master := api.masterClient(req)

	dataset := api.ds.GetDataset(workspace, name, master)
	if dataset == nil {
		WriteStatusError(resp, http.StatusNotFound, fmt.Errorf("Dataset '%v' not found", name))
		return
	}

	dsv, err := dataset.CommitVersion(version)
	if err != nil {
		WriteError(resp, err)
		return
	}

	resp.WriteEntity(dsv)
}

func (api *API) allDatasets(req *restful.Request, resp *restful.Response) {
	sets := api.ds.ListDatasets("")
	ds := types.DataSetList{}
	for _, d := range sets {
		ds.Datasets = append(ds.Datasets, types.Dataset{Name: d.Name, Workspace: d.Workspace})
	}
	if len(ds.Datasets) == 0 {
		ds.Datasets = make([]types.Dataset, 0)
	}
	sort.Sort(ds)
	resp.WriteEntity(ds)
}

func (api *API) datasets(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")

	sets := api.ds.ListDatasets(workspace)
	ds := types.DataSetList{}
	for _, d := range sets {
		ds.Datasets = append(ds.Datasets, types.Dataset{Name: d.Name, Workspace: d.Workspace})
	}
	if len(ds.Datasets) == 0 {
		ds.Datasets = make([]types.Dataset, 0)
	}
	sort.Sort(ds)
	resp.WriteEntity(ds)
}

func (api API) fsCacheKey(dataset *datasets.Dataset, version string) string {
	return dataset.Workspace + dataset.Name + version + "-fs"
}

func (api *API) getFS(dataset *datasets.Dataset, version string) (fs *plukio.ChunkedFileFS, err error) {
	fsRaw := api.fsCache.GetRaw(api.fsCacheKey(dataset, version))
	if fsRaw == nil {
		fs, err = dataset.GetFSStructure(version)
		if err != nil {
			return nil, errors.NewStatus(http.StatusNotFound, err.Error())
		}
	} else {
		fs = fsRaw.(*plukio.ChunkedFileFS)
	}
	api.fsCache.SetRaw(api.fsCacheKey(dataset, version), fs)
	return fs.Clone(), err
}

func (api *API) cacheFS(dataset *datasets.Dataset, versions []string) {
	for _, v := range versions {
		logrus.Infof("Caching FS %v:%v...", dataset.Name, v)
		_, err := api.getFS(dataset, v)
		if err != nil {
			logrus.Error(err)
			return
		}
		logrus.Infof("Successfully cached FS %v:%v.", dataset.Name, v)
	}
}

func (api *API) dealerClient(req *restful.Request) (*dealerclient.Client, error) {
	return dealerclient.NewClient(utils.AuthValidationURL(), &dealerclient.AuthOpts{Headers: req.Request.Header})
}

func (api *API) createDatasetOnDealer(req *restful.Request, ws, name string, public bool) error {
	if utils.AuthValidationURL() == "" {
		return nil
	}

	dealer, err := api.dealerClient(req)
	if err != nil {
		return err
	}

	dealerDatasets, err := dealer.ListDatasets(ws)
	if err != nil {
		return err
	}
	for _, ds := range dealerDatasets {
		if ds.Name == name {
			// Already exists
			return nil
		}
	}

	return dealer.CreateDataset(ws, name, public)
}
