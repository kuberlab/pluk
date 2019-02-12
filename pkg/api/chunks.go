package api

import (
	"io"
	"net/http"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	plukio "github.com/kuberlab/pluk/pkg/io"
)

func (api *API) checkChunk(req *restful.Request, resp *restful.Response) {
	hash := req.PathParameter("hash")
	versionRaw := req.PathParameter("version")
	var version int64 = 0
	if versionRaw != "" {
		version, _ = strconv.ParseInt(versionRaw, 10, 8)
	}

	chunkCheck, err := plukio.CheckChunk(hash, byte(version))
	if err != nil {
		WriteError(resp, err)
		return
	}
	resp.WriteEntity(chunkCheck)
}

func (api *API) downloadChunk(req *restful.Request, resp *restful.Response) {
	hash := req.PathParameter("hash")
	versionRaw := req.PathParameter("version")
	var version int64 = 0
	if versionRaw != "" {
		version, _ = strconv.ParseInt(versionRaw, 10, 8)
	}
	file, err := plukio.GetChunkByHash(hash, byte(version))
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
	versionRaw := req.PathParameter("version")
	var version int64 = 0
	if versionRaw != "" {
		version, _ = strconv.ParseInt(versionRaw, 10, 8)
	}

	if err := plukio.SaveChunk(hash, byte(version), req.Request.Body, true); err != nil {
		WriteStatusError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.WriteHeader(http.StatusCreated)
	resp.Write([]byte("Ok!\n"))
}
