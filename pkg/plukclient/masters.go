package plukclient

import (
	"io"

	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

type MultiMasterClient struct {
	Masters []string
}

func NewMultiClient() plukio.PlukClient {
	masters := utils.Masters()
	return &MultiMasterClient{Masters: masters}
}

func (c *MultiMasterClient) ListDatasets(workspace string) (res *types.DataSetList, err error) {
	var cl plukio.PlukClient
	for _, base := range c.Masters {
		cl, err = NewClient(base, nil)
		if err != nil {
			return nil, err
		}
		res, err = cl.ListDatasets(workspace)
		if err != nil {
			continue
		}
		return res, err
	}
	return nil, err
}

func (c *MultiMasterClient) ListVersions(workspace, datasetName string) (res *types.VersionList, err error) {
	var cl plukio.PlukClient
	for _, base := range c.Masters {
		cl, err = NewClient(base, nil)
		if err != nil {
			return nil, err
		}
		res, err = cl.ListVersions(workspace, datasetName)
		if err != nil {
			continue
		}
		return res, err
	}
	return nil, err
}

func (c *MultiMasterClient) DownloadChunk(hash string) (reader io.ReadCloser, err error) {
	var cl plukio.PlukClient
	for _, base := range c.Masters {
		cl, err = NewClient(base, nil)
		if err != nil {
			return nil, err
		}
		reader, err = cl.DownloadChunk(hash)
		if err != nil {
			continue
		}
		return reader, err
	}
	return nil, err
}

func (c *MultiMasterClient) GetFSStructure(workspace, name, version string) (fs *plukio.ChunkedFileFS, err error) {
	var cl plukio.PlukClient
	for _, base := range c.Masters {
		cl, err = NewClient(base, nil)
		if err != nil {
			return nil, err
		}
		fs, err = cl.GetFSStructure(workspace, name, version)
		if err != nil {
			continue
		}
		return fs, err
	}
	return nil, err
}

func (c *MultiMasterClient) DownloadDataset(workspace, name, version string, w io.Writer) (err error) {
	var cl plukio.PlukClient
	for _, base := range c.Masters {
		cl, err = NewClient(base, nil)
		if err != nil {
			return err
		}
		err = cl.DownloadDataset(workspace, name, version, w)
		if err != nil {
			continue
		}
		return err
	}
	return err
}

func (c *MultiMasterClient) SaveChunk(hash string, data []byte) (err error) {
	var cl plukio.PlukClient
	for _, base := range c.Masters {
		cl, err = NewClient(base, nil)
		if err != nil {
			return err
		}
		err = cl.SaveChunk(hash, data)
		if err != nil {
			continue
		}
		return err
	}
	return err
}

func (c *MultiMasterClient) CommitFileStructure(structure types.FileStructure, workspace, name, version string) (err error) {
	var cl plukio.PlukClient
	for _, base := range c.Masters {
		cl, err = NewClient(base, nil)
		if err != nil {
			return err
		}
		err = cl.CommitFileStructure(structure, workspace, name, version)
		if err != nil {
			continue
		}
		return err
	}
	return err
}

func (c *MultiMasterClient) CheckChunk(hash string) (res *types.CheckChunkResponse, err error) {
	var cl plukio.PlukClient
	for _, base := range c.Masters {
		cl, err = NewClient(base, nil)
		if err != nil {
			return nil, err
		}
		res, err = cl.CheckChunk(hash)
		if err != nil {
			continue
		}
		return nil, err
	}
	return nil, err
}

func (c *MultiMasterClient) DeleteDataset(workspace, name string) (err error) {
	var cl plukio.PlukClient
	for _, base := range c.Masters {
		cl, err = NewClient(base, nil)
		if err != nil {
			return err
		}
		err = cl.DeleteDataset(workspace, name)
		if err != nil {
			continue
		}
		return err
	}
	return err
}

func (c *MultiMasterClient) DeleteVersion(workspace, name, version string) (err error) {
	var cl plukio.PlukClient
	for _, base := range c.Masters {
		cl, err = NewClient(base, nil)
		if err != nil {
			return err
		}
		err = cl.DeleteVersion(workspace, name, version)
		if err != nil {
			continue
		}
		return err
	}
	return err
}