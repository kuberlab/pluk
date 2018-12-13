package api

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/dealerclient"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/datasets"
	"github.com/kuberlab/pluk/pkg/gc"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/plukclient"
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

func (api *API) allDatasets(req *restful.Request, resp *restful.Response) {
	sets := api.ds.ListDatasets(currentType(req), "")
	ds := types.DataSetList{}
	for _, d := range sets {
		ds.Items = append(ds.Items, types.Dataset{Name: d.Name, Workspace: d.Workspace})
	}
	if len(ds.Items) == 0 {
		ds.Items = make([]types.Dataset, 0)
	}
	sort.Sort(ds)
	resp.WriteEntity(ds)
}

func (api *API) datasets(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")

	sets := api.ds.ListDatasets(currentType(req), workspace)
	ds := types.DataSetList{}
	for _, d := range sets {
		ds.Items = append(
			ds.Items,
			types.Dataset{Name: d.Name, Workspace: d.Workspace, Type: d.Type},
		)
	}
	if len(ds.Items) == 0 {
		ds.Items = make([]types.Dataset, 0)
	}
	sort.Sort(ds)
	resp.WriteEntity(ds)
}

func (api *API) downloadDataset(req *restful.Request, resp *restful.Response) {
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
	master := api.masterClient(req)

	dataset := api.ds.GetDataset(currentType(req), workspace, name, master)
	if dataset == nil {
		WriteError(resp, EntityNotFoundError(req, name))
		return
	}

	err := api.checkEntityExists(req, workspace, name)
	if err != nil {
		WriteError(resp, err)
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

func (api *API) getDataset(req *restful.Request, resp *restful.Response) {
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
	master := api.masterClient(req)

	dataset := api.ds.GetDataset(currentType(req), workspace, name, master)
	if dataset == nil {
		WriteError(resp, EntityNotFoundError(req, name))
		return
	}

	resp.WriteEntity(dataset)
}

func (api *API) datasetTarSize(req *restful.Request, resp *restful.Response) {
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
	master := api.masterClient(req)

	dataset := api.ds.GetDataset(currentType(req), workspace, name, master)
	if dataset == nil {
		WriteError(resp, EntityNotFoundError(req, name))
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

func (api *API) deleteDataset(req *restful.Request, resp *restful.Response) {
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
	master := api.masterClient(req)

	ds := api.ds.GetDataset(currentType(req), workspace, name, master)

	api.invalidateCache(ds)
	err := api.ds.DeleteDataset(currentType(req), workspace, name, master, true)
	if err != nil {
		WriteError(resp, err)
		return
	}

	if utils.AuthValidationURL() != "" {

	}

	resp.WriteHeader(http.StatusNoContent)
}

func (api *API) createDataset(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")
	master := api.masterClient(req)

	dataset := api.ds.GetDataset(currentType(req), workspace, name, master)
	if dataset != nil {
		WriteStatusError(resp, http.StatusConflict, fmt.Errorf("%v '%v' already exists", strings.Title(currentType(req)), name))
		return
	}

	// Wait
	gc.WaitGCCompleted()

	if ds, err := api.ds.NewDataset(currentType(req), workspace, name, master); err != nil {
		WriteError(resp, err)
		return
	} else {
		resp.WriteHeaderAndEntity(http.StatusCreated, ds)
	}
}

func (api *API) forkDataset(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")
	targetWS := req.PathParameter("targetWorkspace")
	forceRaw := req.QueryParameter("force")
	force, _ := strconv.ParseBool(forceRaw)
	targetName := req.QueryParameter("name")
	if targetName == "" {
		targetName = name
	}
	targetType := req.QueryParameter("type")
	if targetType == "" {
		targetType = currentType(req)
	}

	if _, ok := plukclient.AllowedTypes[targetType]; !ok {
		msg := fmt.Sprintf("Wrong entity type: Must be one of %v", plukclient.AllowedTypesList())
		WriteErrorString(resp, http.StatusBadRequest, msg)
		return
	}

	master := api.masterClient(req)

	checkTarget := api.ds.GetDataset(targetType, targetWS, targetName, master)
	if checkTarget != nil && force {
		// Clean old dataset
		err := api.ds.DeleteDataset(targetType, targetWS, targetName, master, true)
		if err != nil {
			WriteError(resp, err)
			return
		}
		api.invalidateCache(checkTarget)
		time.Sleep(time.Millisecond * 30)
		gc.WaitGCCompleted()
	}

	sem.Acquire(ctx, 1)
	defer sem.Release(1)

	src := types.Dataset{Workspace: workspace, Name: name, Type: currentType(req)}
	target := types.Dataset{Workspace: targetWS, Name: targetName, Type: targetType}

	dataset, err := api.ds.ForkDataset(src, target, master)
	if err != nil {
		WriteError(resp, err)
		return
	}

	resp.WriteHeaderAndEntity(http.StatusCreated, dataset)
}

func (api *API) fsCacheKey(dataset *datasets.Dataset, version string) string {
	return dataset.Type + dataset.Workspace + dataset.Name + version + "-fs"
}

func (api *API) getFS(dataset *datasets.Dataset, version string) (fs *plukio.ChunkedFileFS, err error) {
	fsRaw := api.fsCache.GetRaw(api.fsCacheKey(dataset, version))
	if fsRaw == nil {
		logrus.Infof("Caching FS %v:%v...", dataset.Name, version)
		fs, err = dataset.GetFSStructure(version)
		if err != nil {
			return nil, errors.NewStatus(http.StatusNotFound, err.Error())
		}
		api.fsCache.SetRaw(api.fsCacheKey(dataset, version), fs)
		logrus.Infof("Successfully cached FS %v:%v.", dataset.Name, version)
	} else {
		fs = fsRaw.(*plukio.ChunkedFileFS)
	}

	return fs.Clone(), err
}

func (api *API) cacheFS(dataset *datasets.Dataset, versions []string) {
	for _, v := range versions {
		_, err := api.getFS(dataset, v)
		if err != nil {
			logrus.Error(err)
			return
		}
	}
}

func (api *API) invalidateCache(ds *datasets.Dataset) {
	if ds == nil {
		return
	}
	vs, err := ds.Versions()
	if err != nil {
		return
	}
	for _, v := range vs {
		// Invalidate cache
		api.fsCache.Cache.Delete(api.fsCacheKey(ds, v.Version))
	}
}

func (api *API) invalidateVersionCache(ds *datasets.Dataset, version string) {
	// Invalidate cache
	api.fsCache.Cache.Delete(api.fsCacheKey(ds, version))
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

	var listMethod func(string) ([]dealerclient.Dataset, error)
	var createMethod func(string, string, bool) error

	switch currentType(req) {
	case "dataset":
		listMethod = dealer.ListDatasets
		createMethod = dealer.CreateDataset
	case "model":
		listMethod = dealer.ListModels
		createMethod = dealer.CreateModel
	}

	dealerDatasets, err := listMethod(ws)
	if err != nil {
		return err
	}
	for _, ds := range dealerDatasets {
		if ds.Name == name {
			// Already exists
			return nil
		}
	}

	return createMethod(ws, name, public)
}

func (api *API) deleteDatasetOnDealer(req *restful.Request, ws, name string) error {
	dealer, err := api.dealerClient(req)
	if err != nil {
		return errors.NewStatus(
			http.StatusBadRequest,
			fmt.Sprintf("Can not delete %v '%v' from API: %v", currentType(req), name, err),
		)
	}

	var deleteMethod func(string, string) error

	switch currentType(req) {
	case "dataset":
		deleteMethod = dealer.DeleteDataset
	case "model":
		deleteMethod = dealer.DeleteModel
	}

	// Skip error
	err = deleteMethod(ws, name)
	if err != nil {
		logrus.Error(err)
	}
	return nil
}
