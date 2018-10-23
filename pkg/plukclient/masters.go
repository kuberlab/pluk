package plukclient

import (
	"io"
	"net/http"
	"strings"

	"github.com/Sirupsen/logrus"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

type MultiMasterClient struct {
	Masters     []string
	baseClients []plukio.PlukClient
	AuthOpts    AuthOpts
}

func NewInternalMasterClient() plukio.PlukClient {
	masters := utils.Masters()
	mClient := &MultiMasterClient{
		Masters: masters,
		AuthOpts: AuthOpts{
			InternalKey:        utils.InternalKey(),
			InsecureSkipVerify: true,
		},
	}
	mClient.initAllClients()
	return mClient
}

func NewMasterClientWithSecret(workspace, secret string) plukio.PlukClient {
	masters := utils.Masters()
	mClient := &MultiMasterClient{Masters: masters, AuthOpts: AuthOpts{Workspace: workspace, Secret: secret}}
	mClient.initAllClients()
	return mClient
}

func NewMasterClientFromHeaders(headers http.Header) plukio.PlukClient {
	masters := utils.Masters()
	auth := AuthOpts{
		Cookie:             headers.Get("Cookie"),
		Workspace:          headers.Get("X-Workspace-Name"),
		Secret:             headers.Get("X-Workspace-Secret"),
		Token:              strings.TrimPrefix(headers.Get("Authorization"), "Bearer "),
		InsecureSkipVerify: true,
	}
	mClient := &MultiMasterClient{Masters: masters, AuthOpts: auth}
	mClient.initAllClients()
	return mClient
}

func (c *MultiMasterClient) initAllClients() {
	c.baseClients = make([]plukio.PlukClient, 0)
	for _, m := range c.Masters {
		cl, err := c.initBaseClient(m)
		if err == nil {
			c.baseClients = append(c.baseClients, cl)
		}
	}
}

func (c *MultiMasterClient) initBaseClient(baseURL string) (plukio.PlukClient, error) {
	return NewClient(baseURL, &c.AuthOpts)
}

func (c *MultiMasterClient) PrepareWebsocket() error {
	return nil
}

func (c *MultiMasterClient) SaveChunkWebsocket(hash string, data []byte) (err error) {
	return
}

func (c *MultiMasterClient) CheckChunkWebsocket(hash string) (res *types.ChunkCheck, err error) {
	return
}

func (c *MultiMasterClient) Close() error {
	return nil
}

func (c *MultiMasterClient) CheckWorkspace(workspace string) (ws *types.Workspace, err error) {
	for _, cl := range c.baseClients {
		ws, err = cl.CheckWorkspace(workspace)
		if err != nil {
			continue
		}
		return ws, err
	}
	return nil, err
}

func (c *MultiMasterClient) CheckEntity(entityType, workspace, dataset string, write bool) (ds *types.Dataset, err error) {
	for _, cl := range c.baseClients {
		ds, err = cl.CheckEntity(entityType, workspace, dataset, write)
		if err != nil {
			continue
		}
		return ds, err
	}
	return nil, err
}

func (c *MultiMasterClient) ListEntities(entityType, workspace string) (res *types.DataSetList, err error) {
	for _, cl := range c.baseClients {
		if err != nil {
			return nil, err
		}
		res, err = cl.ListEntities(entityType, workspace)
		if err != nil {
			continue
		}
		return res, err
	}
	return nil, err
}

func (c *MultiMasterClient) ListVersions(entityType, workspace, datasetName string) (res *types.VersionList, err error) {
	for _, cl := range c.baseClients {
		if err != nil {
			return nil, err
		}
		res, err = cl.ListVersions(entityType, workspace, datasetName)
		if err != nil {
			continue
		}
		return res, err
	}
	return nil, err
}

func (c *MultiMasterClient) DownloadChunk(hash string) (reader io.ReadCloser, err error) {
	for _, cl := range c.baseClients {
		reader, err = cl.DownloadChunk(hash)
		if err != nil {
			continue
		}
		return reader, err
	}
	return nil, err
}

func (c *MultiMasterClient) GetFSStructure(entityType, workspace, name, version string) (fs *plukio.ChunkedFileFS, err error) {
	for _, cl := range c.baseClients {
		if err != nil {
			return nil, err
		}
		fs, err = cl.GetFSStructure(entityType, workspace, name, version)
		if err != nil {
			continue
		}
		return fs, err
	}
	return nil, err
}

func (c *MultiMasterClient) DownloadEntity(entityType, workspace, name, version string, w io.Writer) (err error) {
	for _, cl := range c.baseClients {
		if err != nil {
			return err
		}
		err = cl.DownloadEntity(entityType, workspace, name, version, w)
		if err != nil {
			continue
		}
		return err
	}
	return err
}

func (c *MultiMasterClient) EntityTarSize(entityType, workspace, name, version string) (res int64, err error) {
	for _, cl := range c.baseClients {
		if err != nil {
			return 0, err
		}
		res, err = cl.EntityTarSize(entityType, workspace, name, version)
		if err != nil {
			continue
		}
		return 0, err
	}
	return res, err
}

func (c *MultiMasterClient) SaveChunk(hash string, data []byte) (err error) {
	for i, cl := range c.baseClients {
		if err != nil {
			return err
		}
		err = cl.SaveChunk(hash, data)
		if err != nil {
			logrus.Errorf("Failed save chunk to %v", c.Masters[i])
			return
		}
	}
	return err
}

func (c *MultiMasterClient) SaveFileStructure(structure types.FileStructure, entityType, workspace, name, version string, create bool, publish bool) (err error) {
	for i, cl := range c.baseClients {
		if err != nil {
			return err
		}
		err = cl.SaveFileStructure(structure, entityType, workspace, name, version, create, publish)
		if err != nil {
			logrus.Errorf("Failed save FS to %v", c.Masters[i])
		}
		continue
	}
	return err
}

func (c *MultiMasterClient) CheckChunk(hash string) (res *types.ChunkCheck, err error) {
	for _, cl := range c.baseClients {
		if err != nil {
			return nil, err
		}
		res, err = cl.CheckChunk(hash)
		if err != nil {
			continue
		}
		return res, err
	}
	return nil, err
}

func (c *MultiMasterClient) DeleteEntity(entityType, workspace, name string, force bool) (err error) {
	for _, cl := range c.baseClients {
		if err != nil {
			return err
		}
		err = cl.DeleteEntity(entityType, workspace, name, force)
		if err != nil {
			continue
		}
		return err
	}
	return err
}

func (c *MultiMasterClient) DeleteVersion(entityType, workspace, name, version string) (err error) {
	for _, cl := range c.baseClients {
		if err != nil {
			return err
		}
		err = cl.DeleteVersion(entityType, workspace, name, version)
		if err != nil {
			continue
		}
		return err
	}
	return err
}

func (c *MultiMasterClient) WebdavAuth(user, pass, path string) (yes bool, err error) {
	for _, cl := range c.baseClients {
		if err != nil {
			return false, err
		}
		yes, err = cl.WebdavAuth(user, pass, path)
		if err != nil {
			continue
		}
		return yes, err
	}
	return yes, err
}
