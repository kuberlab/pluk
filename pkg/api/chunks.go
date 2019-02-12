package api

import (
	"io"
	"net/http"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	plukio "github.com/kuberlab/pluk/pkg/io"
)

func (api *API) chunkVersion(req *restful.Request) byte {
	versionRaw := req.PathParameter("version")
	// Faster
	if versionRaw == "1" {
		return 1
	}
	if versionRaw == "" {
		return 0
	}
	var version int64 = 0
	if versionRaw != "" {
		version, _ = strconv.ParseInt(versionRaw, 10, 8)
	}
	return byte(version)
}

func (api *API) checkChunk(req *restful.Request, resp *restful.Response) {
	hash := req.PathParameter("hash")

	chunkCheck, err := plukio.CheckChunk(hash, api.chunkVersion(req))
	if err != nil {
		WriteError(resp, err)
		return
	}
	resp.WriteEntity(chunkCheck)
}

func (api *API) downloadChunk(req *restful.Request, resp *restful.Response) {
	hash := req.PathParameter("hash")
	file, err := plukio.GetChunkByHash(hash, api.chunkVersion(req))
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

	if err := plukio.SaveChunk(hash, api.chunkVersion(req), req.Request.Body, true); err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.WriteHeader(http.StatusCreated)
	resp.Write([]byte("Ok!\n"))
}
