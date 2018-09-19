package api

import (
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/pluk/pkg/utils"
)

func (api *API) runGC(req *restful.Request, resp *restful.Response) {
	utils.GCChan <- "Run GC by API request"
	resp.Write([]byte("GC started!\n"))
}

func (api *API) runClearChunks(req *restful.Request, resp *restful.Response) {
	utils.GCClearChunks <- "Run by API request"
	resp.Write([]byte("Clear chunks started!\n"))
}
