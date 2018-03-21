package api

import (
	"fmt"
	"net/http"

	"github.com/emicklei/go-restful"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

func (api *API) checkWorkspace(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")

	u := utils.AuthValidationURL()
	if u == "" {
		resp.WriteEntity(&types.Workspace{Name: workspace})
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

func (api *API) checkDataset(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	dataset := req.PathParameter("dataset")

	u := utils.AuthValidationURL()
	if u == "" {
		resp.WriteEntity(&types.Dataset{Name: dataset, Workspace: workspace})
		return
	}

	dealer, err := api.dealerClient(req)
	if err != nil {
		WriteError(resp, err)
		return
	}
	dss, err := dealer.ListDatasets(workspace)
	if err != nil {
		WriteError(resp, err)
		return
	}
	for _, ds := range dss {
		if ds.Name == dataset {
			resp.WriteEntity(ds)
			return
		}
	}
	err = dealer.CheckDataset(workspace, dataset)
	if err != nil {
		WriteError(resp, err)
		return
	}
	WriteErrorString(resp, http.StatusNotFound, fmt.Sprintf("Dataset %v not found", dataset))
}
