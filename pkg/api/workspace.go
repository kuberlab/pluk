package api

import (
	"fmt"
	"net/http"

	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/dealerclient"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
	"strconv"
)

func (api *API) checkWorkspace(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")

	u := utils.AuthValidationURL()
	if u == "" && !utils.HasMasters() {
		resp.WriteEntity(&types.Workspace{Name: workspace})
		return
	}

	if u == "" && utils.HasMasters() {
		// Request master.
		masters := plukclient.NewMasterClientFromHeaders(req.Request.Header)
		ws, err := masters.CheckWorkspace(workspace)
		if err != nil {
			WriteError(resp, err)
			return
		}
		resp.WriteEntity(ws)
		return
	}

	dealer, err := api.dealerClient(req)
	if err != nil {
		WriteError(resp, err)
		return
	}
	ws, err := dealer.GetWorkspace(workspace)
	if err != nil {
		WriteError(resp, err)
		return
	}
	resp.WriteEntity(ws)
}

func (api *API) checkEntityAccess(req *restful.Request, write bool) (*types.Dataset, error) {
	workspace := req.PathParameter("workspace")
	dataset := req.PathParameter("dataset")

	if dataset == "" {
		dataset = req.PathParameter("name")
	}

	u := utils.AuthValidationURL()
	if u == "" && !utils.HasMasters() {
		return &types.Dataset{
			Name:      dataset,
			Workspace: workspace,
			Type:      currentType(req),
		}, nil
	}

	if u == "" && utils.HasMasters() {
		// Request master.
		masters := plukclient.NewMasterClientFromHeaders(req.Request.Header)
		ds, err := masters.CheckEntity(currentType(req), workspace, dataset, write)
		if err != nil {
			return nil, err
		}
		return ds, nil
	}

	dealer, err := api.dealerClient(req)
	if err != nil {
		return nil, err
	}

	var listMethod func(string) ([]dealerclient.Dataset, error)
	var checkMethod func(string, string) error
	switch currentType(req) {
	case "dataset":
		listMethod = dealer.ListDatasets
		checkMethod = dealer.CheckDataset
	case "model":
		listMethod = dealer.ListModels
		checkMethod = dealer.CheckModel

	}

	dss, err := listMethod(workspace)
	if err != nil {
		return nil, err
	}
	for _, ds := range dss {
		if ds.Name == dataset {
			// we found dataset; need to check write permissions
			if write {
				err = checkMethod(workspace, "dataset-which-doesnot-exist")
				if err != nil {
					return nil, err
				}
			}
			return &types.Dataset{
				Type:      currentType(req),
				Workspace: ds.WorkspaceName,
				Name:      ds.Name,
			}, nil
		}
	}
	// Didn't find dataset; check write permission
	if write {
		err = checkMethod(workspace, dataset)
		if err != nil {
			return nil, err
		}
	}
	return nil, errors.NewStatus(
		http.StatusNotFound,
		fmt.Sprintf("%v %v not found", currentType(req), dataset),
	)
}

func (api *API) checkDataset(req *restful.Request, resp *restful.Response) {
	writeRaw := req.QueryParameter("write")
	write, _ := strconv.ParseBool(writeRaw)

	ds, err := api.checkEntityAccess(req, write)
	if err != nil {
		WriteError(resp, err)
		return
	}
	resp.WriteEntity(ds)
}
