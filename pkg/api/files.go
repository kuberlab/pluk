package api

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/datasets"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
	"mime"
	"path"
)

func (api *API) fsReadDir(req *restful.Request, resp *restful.Response) {
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
	path := req.PathParameter("path")
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

	result, err := fs.Readdir(path, 0)
	if err != nil {
		WriteStatusError(resp, http.StatusNotFound, err)
		return
	}

	resp.WriteEntity(result)
}

func (api *API) fsReadFile(req *restful.Request, resp *restful.Response) {
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
	path := req.PathParameter("path")
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

	file := fs.GetFile(path)
	if file == nil || file.Fstat.IsDir() {
		WriteErrorString(resp, http.StatusNotFound, fmt.Sprintf("No such file: %v", path))
		return
	}

	resp.Header().Add("Content-Length", fmt.Sprintf("%v", file.Size))
	setContentTypeByFile(path, resp)
	resp.ResponseWriter.WriteHeader(http.StatusOK)

	io.Copy(resp, file)
}

func setContentTypeByFile(filepath string, resp *restful.Response) {
	ext := path.Ext(filepath)
	sType := mime.TypeByExtension(ext)
	if sType != "" {
		resp.Header().Add("Content-Type", sType)
	}
}

func (api *API) findDatasetVersion(ds *datasets.Dataset, version string, allowEditing bool) (*types.Version, error) {
	versions, err := ds.Versions()
	if err != nil {
		return nil, errors.NewStatus(http.StatusInternalServerError, err.Error())
	}

	found := false
	var vs types.Version
	for _, v := range versions {
		if v.Version == version {
			vs = v
			found = true
			break
		}
	}

	if !found {
		return nil, errors.NewStatus(http.StatusNotFound, fmt.Sprintf("Dataset version not found: %v", version))
	}

	if !vs.Editing && !allowEditing {
		return nil, errors.NewStatus(http.StatusForbidden, "Dataset already committed")
	}
	return &vs, nil
}

func (api *API) deleteDatasetFile(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")
	version := req.PathParameter("version")
	filepath := req.PathParameter("path")
	master := api.masterClient(req)

	dataset := api.ds.GetDataset(workspace, name, master)
	if dataset == nil {
		WriteStatusError(resp, http.StatusNotFound, fmt.Errorf("Dataset '%v' not found", name))
		return
	}

	if filepath == "" {
		WriteStatusError(resp, http.StatusBadRequest, fmt.Errorf("Provide path"))
		return
	}

	_, err := api.findDatasetVersion(dataset, version, false)
	if err != nil {
		WriteError(resp, err)
		return
	}

	//_, err = api.mgr.GetFile(workspace, name, filepath, version)
	//if err != nil {
	//	// File does not exists
	//	WriteErrorString(
	//		resp,
	//		http.StatusNotFound,
	//		fmt.Sprintf("File %v for %v/%v:%v not found", filepath, workspace, name, version),
	//	)
	//	return
	//}
	tx := api.mgr.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	if err = datasets.DeleteFiles(tx, workspace, name, version, filepath); err != nil {
		WriteError(resp, err)
		return
	}
	// Invalidate dataset size
	err = tx.UpdateDatasetVersionSize(workspace, name, version)
	if err != nil {
		WriteError(resp, err)
		return
	}
	// Invalidate cache
	api.fsCache.Cache.Delete(api.fsCacheKey(dataset, version))
	resp.WriteHeader(http.StatusNoContent)
}

func (api *API) uploadDatasetFile(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")
	version := req.PathParameter("version")
	filepath := req.PathParameter("path")
	master := api.masterClient(req)

	dataset := api.ds.GetDataset(workspace, name, master)
	if dataset == nil {
		WriteStatusError(resp, http.StatusNotFound, fmt.Errorf("Dataset '%v' not found", name))
		return
	}

	if filepath == "" {
		WriteStatusError(resp, http.StatusBadRequest, fmt.Errorf("Provide path"))
		return
	}

	_, err := api.findDatasetVersion(dataset, version, false)
	if err != nil {
		WriteError(resp, err)
		return
	}

	_, err = api.mgr.GetFile(workspace, name, filepath, version)
	if err == nil {
		// File exists, need overwrite
		// TODO: overwrite
		// Delete related chunks
		if err = datasets.DeleteFiles(api.mgr, workspace, name, version, filepath); err != nil {
			WriteError(resp, err)
			return
		}

	}

	f := &types.HashedFile{Path: filepath, Mode: 0644, ModeTime: time.Now(), Hashes: make([]types.Hash, 0)}
	var total int64 = 0
	chunkSize := 1024000
	reader := utils.NewPreciseReader(req.Request.Body)
	defer req.Request.Body.Close()
	for {
		buf := make([]byte, chunkSize)
		read, errRead := reader.Read(buf)
		if errRead != nil {
			if errRead != io.EOF {
				WriteError(resp, errRead)
				return
			}
		}
		total += int64(read)

		// Calc hash
		hash := utils.CalcHash(buf)
		// Check and save
		check, err := plukio.CheckChunk(hash)
		if err != nil {
			WriteError(resp, err)
			return
		}
		f.Hashes = append(f.Hashes, types.Hash{Hash: hash, Size: int64(read)})

		if check.Exists && int(check.Size) == read {
			if errRead == io.EOF {
				// Nothing to read or save
				break
			}
			// Skip
			continue
		}

		if err = plukio.SaveChunk(hash, ioutil.NopCloser(bytes.NewBuffer(buf[:read])), true); err != nil {
			WriteError(resp, err)
			return
		}

		if errRead == io.EOF {
			break
		}
	}

	f.Size = total
	fs := types.FileStructure{Files: []*types.HashedFile{f}}

	if err := dataset.Save(fs, version, "", false, false, true); err != nil {
		WriteError(resp, err)
		return
	}
	// Invalidate cache
	api.fsCache.Cache.Delete(api.fsCacheKey(dataset, version))
	resp.WriteHeaderAndEntity(http.StatusCreated, f)
}
