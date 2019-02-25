package api

import (
	"fmt"
	"net/http"
	"sort"
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
	sets, err := api.ds.ListDatasets(currentType(req), "")
	if err != nil {
		WriteError(resp, err)
		return
	}
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

	sets, err := api.ds.ListDatasets(currentType(req), workspace)
	if err != nil {
		WriteError(resp, err)
		return
	}
	ds := types.DataSetList{}
	for _, d := range sets {
		ds.Items = append(
			ds.Items,
			types.Dataset{Name: d.Name, Workspace: d.Workspace, DType: d.Type},
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

	dataset, err := api.ds.GetDataset(currentType(req), workspace, name, master)
	if err != nil {
		WriteError(resp, EntityNotFoundError(req, name, err))
		return
	}

	err = api.checkEntityExists(req, workspace, name)
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

	dataset, err := api.ds.GetDataset(currentType(req), workspace, name, master)
	if err != nil {
		WriteError(resp, EntityNotFoundError(req, name, err))
		return
	}

	resp.WriteEntity(dataset)
}

func (api *API) datasetTarSize(req *restful.Request, resp *restful.Response) {
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
	master := api.masterClient(req)

	dataset, err := api.ds.GetDataset(currentType(req), workspace, name, master)
	if err != nil {
		WriteError(resp, EntityNotFoundError(req, name, err))
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

	acquireConcurrency()
	defer releaseConcurrency()
	ds, _ := api.ds.GetDataset(currentType(req), workspace, name, master)

	api.invalidateCache(ds)
	err := api.ds.DeleteDataset(currentType(req), workspace, name, master, true)
	if err != nil {
		WriteError(resp, err)
		return
	}

	if utils.AuthValidationURL() != "" {
		// The below causes kind of "recursive" deleting
		//err = api.deleteDatasetOnDealer(req, workspace, name)
		//if err != nil {
		//	WriteError(resp, err)
		//	return
		//}
	}

	resp.WriteHeader(http.StatusNoContent)
}

func (api *API) createDataset(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")
	master := api.masterClient(req)

	_, err := api.ds.GetDataset(currentType(req), workspace, name, master)
	if err == nil {
		WriteError(resp, AlreadyExistsError(req, name))
		return
	}

	// Wait
	acquireConcurrency()
	defer releaseConcurrency()
	//gc.WaitGCCompleted()

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
	force := getBoolQueryParam(req, "force")
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

	checkTarget, _ := api.ds.GetDataset(targetType, targetWS, targetName, master)
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

	acquireConcurrency()
	defer releaseConcurrency()

	src := types.Dataset{Workspace: workspace, Name: name, DType: currentType(req)}
	target := types.Dataset{Workspace: targetWS, Name: targetName, DType: targetType}

	dataset, err := api.ds.ForkDataset(src, target, master)
	if err != nil {
		WriteError(resp, err)
		return
	}

	resp.WriteHeaderAndEntity(http.StatusCreated, dataset)
}

func (api *API) fsCacheKey(dataset *datasets.Dataset, version string) string {
	return api.fsCacheKeyPrefix(dataset) + ":" + version + "-fs"
}

func (api *API) fsCacheKeyPrefix(ds *datasets.Dataset) string {
	return fmt.Sprintf("%v/%v/%v", ds.Type, ds.Workspace, ds.Name)
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

	return fs, err
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
	// Drop all cache by prefix
	prefix := api.fsCacheKeyPrefix(ds)
	for k := range api.fsCache.Cache.Items() {
		if strings.HasPrefix(k, prefix) {
			logrus.Infof("Drop cache for %v", k)
			api.fsCache.Cache.Delete(k)
		}
	}
	//vs, err := ds.Versions()
	//if err != nil {
	//	return
	//}
	//for _, v := range vs {
	//	// Invalidate cache
	//	api.fsCache.Cache.Delete(api.fsCacheKey(ds, v.Version))
	//}
}

func (api *API) invalidateVersionCache(ds *datasets.Dataset, version string) {
	// Invalidate cache
	key := api.fsCacheKey(ds, version)
	logrus.Infof("Drop cache for %v", key)
	api.fsCache.Cache.Delete(key)
}

func (api *API) dealerClient(req *restful.Request) (*dealerclient.Client, error) {
	return dealerclient.NewClient(utils.AuthValidationURL(), &dealerclient.AuthOpts{Headers: req.Request.Header})
}

func (api *API) reportNewVersion(req *restful.Request, version dealerclient.NewVersion) {
	if utils.AuthValidationURL() == "" {
		return
	}

	dealer, err := api.dealerClient(req)
	if err != nil {
		logrus.Error(err)
		return
	}
	err = dealer.ReportNewVersion(version)
	if err != nil {
		logrus.Error(err)
	}
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
