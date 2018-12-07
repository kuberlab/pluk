package api

import (
	"fmt"
	"net/http"
	"strconv"

	"compress/gzip"
	"encoding/gob"
	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/pluk/pkg/gc"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

func (api *API) getDatasetFS(req *restful.Request, resp *restful.Response) {
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
	format := req.QueryParameter("format")
	if format == "" {
		format = "json"
	}
	master := api.masterClient(req)

	err := api.checkEntityExists(req, workspace, name)
	if err != nil {
		WriteError(resp, err)
		return
	}

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

	switch format {
	case "json":
		resp.WriteEntity(fs)
	case "gob":
		enc := gob.NewEncoder(resp.ResponseWriter)
		enc.Encode(fs)
	case "gobgz":
		w := gzip.NewWriter(resp.ResponseWriter)
		enc := gob.NewEncoder(w)
		enc.Encode(fs)
		w.Close()
	default:
		WriteErrorString(resp, http.StatusBadRequest, "Wrong format, allowed json/gob/gobgz")
	}
	//resp.Header().Add("Content-Type", "application/tar+gzip")
	//resp.Header().Add("Content-Disposition", fmt.Sprintf("attachment;filename=%s-%s.%s.tgz;", workspace, name, version))
}

func (api *API) saveFS(req *restful.Request, resp *restful.Response) {
	comment := req.QueryParameter("comment")
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

	// Wait
	gc.WaitGCCompleted()

	sem.Acquire(ctx, 1)
	defer sem.Release(1)

	dataset, err := api.ds.NewDataset(currentType(req), workspace, name, master)
	if err != nil {
		WriteError(resp, err)
		return
	}
	logrus.Infof("Saving %v for %v/%v:%v...", dataset.Type, workspace, name, version)

	err = dataset.Save(structure, version, comment, create, publish, true)
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	dsv, err := api.mgr.CommitVersion(currentType(req), workspace, name, version, comment)
	if err != nil {
		WriteStatusError(
			resp,
			http.StatusInternalServerError,
			fmt.Errorf("Failed to commit version %v: %v", version, err.Error()),
		)
		return
	}
	logrus.Infof("Done saving %v/%v:%v.", workspace, name, version)

	// Invalidate cache
	api.fsCache.Cache.Delete(api.fsCacheKey(dataset, version))

	if create {
		if err = api.createDatasetOnDealer(req, workspace, name, publish); err != nil {
			WriteStatusError(resp, http.StatusInternalServerError, err)
			return
		}
	}

	resp.WriteHeaderAndEntity(http.StatusCreated, dsv)
}
