package api

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/datasets"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

func (api *API) fsReadDir(req *restful.Request, resp *restful.Response) {
	version := req.PathParameter("version")
	name := req.PathParameter("name")
	workspace := req.PathParameter("workspace")
	filepath := req.PathParameter("path")
	filter := req.QueryParameter("filter")
	master := api.masterClient(req)

	dataset, err := api.ds.GetDataset(currentType(req), workspace, name, master)
	if err != nil {
		WriteError(resp, EntityNotFoundError(req, name, err))
		return
	}
	fs, err := api.getFS(dataset, version, filter)
	if err != nil {
		WriteError(resp, err)
		return
	}

	result, err := fs.ReaddirFiles(filepath, 0)
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
	filepath := req.PathParameter("path")
	master := api.masterClient(req)

	dataset, err := api.ds.GetDataset(currentType(req), workspace, name, master)
	if err != nil {
		WriteError(resp, EntityNotFoundError(req, name, err))
		return
	}
	fs, err := api.getFS(dataset, version, "")
	if err != nil {
		WriteError(resp, err)
		return
	}

	file := fs.GetFile(filepath)
	if file == nil || file.Dir {
		WriteErrorString(resp, http.StatusNotFound, fmt.Sprintf("No such file: %v", filepath))
		return
	}
	file = file.Clone()

	resp.Header().Add("Content-Length", fmt.Sprintf("%v", file.Size))
	setContentTypeByFile(filepath, resp)
	resp.ResponseWriter.WriteHeader(http.StatusOK)

	_, err = io.Copy(resp, file)
	if err != nil {
		logrus.Error(err)
	}
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
		return nil, errors.NewStatus(
			http.StatusNotFound,
			fmt.Sprintf("%v version not found: %v", ds.Type, version),
		)
	}

	if !vs.Editing && !allowEditing {
		return nil, errors.NewStatus(
			http.StatusForbidden,
			fmt.Sprintf("%v already committed", ds.Type),
		)
	}
	return &vs, nil
}

func (api *API) deleteDatasetFile(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")
	version := req.PathParameter("version")
	filepath := req.PathParameter("path")
	master := api.masterClient(req)

	dataset, err := api.ds.GetDataset(currentType(req), workspace, name, master)
	if err != nil {
		WriteError(resp, EntityNotFoundError(req, name, err))
		return
	}

	if filepath == "" {
		WriteStatusError(resp, http.StatusBadRequest, fmt.Errorf("Provide path"))
		return
	}

	_, err = api.findDatasetVersion(dataset, version, false)
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

	acquireConcurrency()
	defer releaseConcurrency()

	tx := api.mgr.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	if err = datasets.DeleteFiles(tx, currentType(req), workspace, name, version, filepath, false, true); err != nil {
		WriteError(resp, err)
		return
	}
	// Invalidate dataset size
	err = tx.UpdateDatasetVersionSize(currentType(req), workspace, name, version)
	if err != nil {
		WriteError(resp, err)
		return
	}
	// Invalidate cache
	api.invalidateVersionCache(dataset, version)
	resp.WriteHeader(http.StatusNoContent)
}

func (api *API) uploadDatasetFile(req *restful.Request, resp *restful.Response) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")
	version := req.PathParameter("version")
	filepath := req.PathParameter("path")
	master := api.masterClient(req)

	acquireConcurrency()
	defer releaseConcurrency()

	dataset, err := api.ds.GetDataset(currentType(req), workspace, name, master)
	if err != nil {
		WriteError(resp, EntityNotFoundError(req, name, err))
		return
	}

	if filepath == "" {
		WriteStatusError(resp, http.StatusBadRequest, fmt.Errorf("Provide path"))
		return
	}

	_, err = api.findDatasetVersion(dataset, version, false)
	if err != nil {
		WriteError(resp, err)
		return
	}

	f, err := api.readAndSaveFile(req, resp)
	if err != nil {
		WriteError(resp, err)
		return
	}
	fs := types.FileStructure{Files: []*types.HashedFile{f}}

	api.lockForSave(workspace, name, version)
	defer api.unlockForSave(workspace, name, version)
	if err := dataset.Save(fs, version, "", false, false, true, true); err != nil {
		WriteError(resp, err)
		return
	}
	// Invalidate cache
	api.invalidateVersionCache(dataset, version)
	resp.WriteHeaderAndEntity(http.StatusCreated, f)
}

func (api *API) lockForSave(ws, ds, version string) {
	key := fmt.Sprintf("%v-%v-%v", ws, ds, version)
	api.lock.Lock()
	defer api.lock.Unlock()
	lock, ok := api.saveLocks[key]
	if ok {
		lock.Lock()
	} else {
		api.saveLocks[key] = &sync.RWMutex{}
		api.saveLocks[key].Lock()
	}
}

func (api *API) unlockForSave(ws, ds, version string) {
	key := fmt.Sprintf("%v-%v-%v", ws, ds, version)
	lock, ok := api.saveLocks[key]
	if ok {
		lock.Unlock()
	}
}

func (api *API) readAndSaveFile(req *restful.Request, resp *restful.Response) (f *types.HashedFile, err error) {
	workspace := req.PathParameter("workspace")
	name := req.PathParameter("name")
	version := req.PathParameter("version")
	filepath := req.PathParameter("path")

	modeRaw := req.QueryParameter("mode")
	modeOct, _ := strconv.ParseUint(modeRaw, 8, 32)
	mode := uint32(modeOct)
	if modeOct == 0 {
		mode = 0644
	}

	tx := api.mgr.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	_, err = tx.GetFile(workspace, name, currentType(req), filepath, version)
	if err == nil {
		// File exists, need overwrite
		// Delete related chunks
		err = datasets.DeleteFiles(
			tx, currentType(req), workspace, name, version, filepath, true, false,
		)
		if err != nil {
			return nil, err
		}

	}

	f = &types.HashedFile{Path: filepath, Mode: os.FileMode(mode), ModeTime: time.Now(), Hashes: make([]types.Hash, 0)}
	var total int64 = 0
	chunkSize := 1024000
	reader := utils.NewPreciseReader(req.Request.Body)
	defer req.Request.Body.Close()
	var check *types.ChunkCheck
	for {
		buf := make([]byte, chunkSize)
		read, errRead := reader.Read(buf)
		if errRead != nil {
			if errRead != io.EOF {
				return nil, err
			}
		}
		total += int64(read)

		// Calc hash
		hash := utils.CalcHash(buf)
		// Check and save
		check, err = plukio.CheckChunk(hash, types.ChunkVersion)
		if err != nil {
			return nil, err
		}
		f.Hashes = append(f.Hashes, types.Hash{Hash: hash, Size: int64(read), Version: types.ChunkVersion})

		if check.Exists && int(check.Size) == read {
			if errRead == io.EOF {
				// Nothing to read or save
				break
			}
			// Skip
			continue
		}

		if _, err = plukio.SaveChunk(hash, types.ChunkVersion, ioutil.NopCloser(bytes.NewBuffer(buf[:read])), true); err != nil {
			return nil, err
		}

		if errRead == io.EOF {
			break
		}
	}

	f.Size = total
	return f, nil
}
