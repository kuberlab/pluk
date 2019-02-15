package api

import (
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/pluk/pkg/db"
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

	//err := api.checkEntityExists(req, workspace, name)
	//if err != nil {
	//	WriteError(resp, err)
	//	return
	//}

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

	switch format {
	case "json":
		resp.PrettyPrint(false)
		resp.WriteEntity(fs)
	case "gob":
		enc := gob.NewEncoder(resp.ResponseWriter)
		_ = enc.Encode(fs)
	case "gobgz":
		w := gzip.NewWriter(resp.ResponseWriter)
		enc := gob.NewEncoder(w)
		_ = enc.Encode(fs)
		w.Close()
	default:
		WriteErrorString(resp, http.StatusBadRequest, "Wrong format, allowed json/gob/gobgz")
		return
	}
	//resp.Header().Add("Content-Type", "application/tar+gzip")
	//resp.Header().Add("Content-Disposition", fmt.Sprintf("attachment;filename=%s-%s.%s.tgz;", workspace, name, version))
}

func (api *API) saveFS(req *restful.Request, resp *restful.Response) {
	comment := req.QueryParameter("comment")
	create := getBoolQueryParam(req, "create")
	publish := getBoolQueryParam(req, "publish")
	editing := getBoolQueryParam(req, "editing")
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
	format := req.QueryParameter("format")
	if format == "" {
		format = "json"
	}
	master := api.masterClient(req)

	var err error
	structure := new(types.FileStructure)
	switch format {
	case "json":
		err = req.ReadEntity(structure)
	case "gobgz":
		rd, err := gzip.NewReader(req.Request.Body)
		defer rd.Close()
		if err != nil {
			WriteError(resp, err)
			return
		}

		dec := gob.NewDecoder(rd)
		err = dec.Decode(structure)
		if err != nil {
			WriteError(resp, err)
			return
		}
	default:
		WriteErrorString(resp, http.StatusBadRequest, "Wrong format: allowed json and gobgz")
		return
	}

	if err != nil {
		WriteStatusError(resp, http.StatusBadRequest, err)
		return
	}

	if err = utils.CheckVersion(version); err != nil {
		WriteStatusError(resp, http.StatusBadRequest, err)
		return
	}

	// Wait
	//gc.WaitGCCompleted()

	acquireConcurrency()
	defer releaseConcurrency()

	dataset, err := api.ds.NewDataset(currentType(req), workspace, name, master)
	if err != nil {
		WriteError(resp, err)
		return
	}
	logrus.Infof("Saving %v for %v/%v:%v...", dataset.Type, workspace, name, version)

	err = dataset.Save(*structure, version, comment, create, publish, editing, true)
	if err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	if !editing {
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
		api.invalidateVersionCache(dataset, version)

		if create {
			if err = api.createDatasetOnDealer(req, workspace, name, publish); err != nil {
				WriteStatusError(resp, http.StatusInternalServerError, err)
				return
			}
		}
		resp.WriteHeaderAndEntity(http.StatusCreated, dsv)
		return
	}

	resp.WriteHeaderAndEntity(
		http.StatusCreated,
		&db.DatasetVersion{
			Version:   version,
			Name:      name,
			Type:      currentType(req),
			Workspace: workspace,
			Editing:   true,
		},
	)
}
