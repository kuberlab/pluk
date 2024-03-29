package plukclient

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/gob"
	"errors"
	"fmt"
	"golang.org/x/sync/semaphore"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/gorilla/websocket"
	"github.com/json-iterator/go"
	liberrs "github.com/kuberlab/lib/pkg/errors"
	libtypes "github.com/kuberlab/lib/pkg/types"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Client struct {
	Client    *http.Client
	BaseURL   *url.URL
	UserAgent string

	auth   *AuthOpts
	wsLock sync.RWMutex
	ws     map[*types.WebsocketClient]bool
}

type AuthOpts struct {
	Token              string
	Cookie             string
	InternalKey        string
	Workspace          string
	Secret             string
	InsecureSkipVerify bool
}

var AllowedTypes = map[string]bool{
	"dataset": true,
	"model":   true,
}

func AllowedTypesList() []string {
	allowed := make([]string, 0)
	for k := range AllowedTypes {
		allowed = append(allowed, k)
	}
	return allowed
}

func NewClient(baseURL string, auth *AuthOpts) (*Client, error) {
	baseURL = strings.TrimSuffix(baseURL, "/")
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	if len(base.Path) < 2 {
		base.Path = "/pluk/v1"
	}
	//if auth.InternalKey != "" {
	//	base.Path = "/internal"
	//}
	// Clone default transport
	var transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 90 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       0 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if base.Scheme == "https" {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: auth.InsecureSkipVerify}
	}
	baseClient := &http.Client{Timeout: time.Hour * 8, Transport: transport}

	return &Client{
		BaseURL:   base,
		Client:    baseClient,
		UserAgent: "go-plukclient/1",
		auth:      auth,
		ws:        make(map[*types.WebsocketClient]bool),
	}, nil
}

func (c *Client) PrepareWebsocket(num int64) error {
	dialer := websocket.Dialer{}
	urlStr := "/websocket-chunks"

	var scheme string
	switch c.BaseURL.Scheme {
	case "http":
		scheme = "ws"
	case "https":
		tlsConf := &tls.Config{
			InsecureSkipVerify: true,
		}
		dialer.TLSClientConfig = tlsConf
		scheme = "wss"
	}
	u := fmt.Sprintf("%v://%v/%v", scheme, c.BaseURL.Host, strings.TrimPrefix(c.BaseURL.Path, "/"))
	u = strings.TrimSuffix(u, "/") + urlStr
	logrus.Debugf("Connect to %v", u)

	sem := semaphore.NewWeighted(num)
	ctx := context.TODO()
	for i := 0; i < int(num); i++ {
		go func() {
			sem.Acquire(ctx, 1)
			conn, resp, err := dialer.Dial(u, c.authHeaders())

			if err != nil {
				logrus.Fatal(err)
			}
			id := resp.Header.Get("Sec-Websocket-Accept")
			ws := types.NewWebsocketClient(conn, id, "0.0.0.0")

			c.wsLock.Lock()
			c.ws[ws] = false
			c.wsLock.Unlock()

			sem.Release(1)
		}()
	}
	time.Sleep(200 * time.Millisecond)
	sem.Acquire(ctx, num)
	return nil
}

func (c *Client) NewRequest(method, urlStr string, body interface{}) (*http.Request, error) {
	u := c.BaseURL.String()
	u = strings.TrimSuffix(u, "/") + urlStr

	var reqBody io.Reader
	if body != nil {
		rd, ok := body.(io.Reader)
		if ok {
			reqBody = rd
		} else {
			buf := new(bytes.Buffer)
			err := json.NewEncoder(buf).Encode(body)
			if err != nil {
				return nil, err
			}
			reqBody = buf
		}
	}

	req, err := http.NewRequest(method, u, reqBody)
	if err != nil {
		return nil, err
	}
	c.setNeededHeaders(req)

	return req, nil
}

func (c *Client) setNeededHeaders(req *http.Request) {
	if c.auth != nil {
		for k, v := range c.authHeaders() {
			req.Header.Set(k, v[0])
		}
	}

	if req.Body != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
}

func (c *Client) authHeaders() http.Header {
	h := make(http.Header)
	if c.auth.Cookie != "" {
		h.Set("Cookie", c.auth.Cookie)
	}
	if c.auth.Token != "" {
		h.Set("Authorization", fmt.Sprintf("Bearer %v", c.auth.Token))
	}
	if c.auth.InternalKey != "" {
		h.Set("Internal", c.auth.InternalKey)
	}
	if c.auth.Workspace != "" {
		h.Set("X-Workspace-Name", c.auth.Workspace)
	}
	if c.auth.Secret != "" {
		h.Set("X-Workspace-Secret", c.auth.Secret)
	}
	return h
}

func (c *Client) CheckWorkspace(workspace string) (*types.Workspace, error) {
	u := fmt.Sprintf("/workspaces/%v", workspace)

	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	res := new(types.Workspace)
	_, err = c.Do(req, res)

	if err != nil {
		return nil, err
	}

	return res, err
}

func (c *Client) PostEntitySpec(entityType, workspace, name string, spec interface{}) error {
	u := fmt.Sprintf("/workspaces/%v/%v/%v/spec", workspace, entityType, name)

	req, err := c.NewRequest("POST", u, spec)
	if err != nil {
		return err
	}
	_, err = c.Do(req, nil)
	return err
}

func (c *Client) PostEntitySpecForVersion(entityType, workspace, name, version string, spec interface{}) error {
	u := fmt.Sprintf("/workspaces/%v/%v/%v/versions/%v/spec", workspace, entityType, name, version)

	req, err := c.NewRequest("POST", u, spec)
	if err != nil {
		return err
	}
	_, err = c.Do(req, nil)
	return err
}

func (c *Client) CheckEntityPermission(entityType, workspace, name string, write bool) (*types.Dataset, error) {
	u := fmt.Sprintf("/workspaces/%v/%v/%v/permission", workspace, entityType, name)

	if write {
		u += "?write=true"
	}

	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	res := new(types.Dataset)
	_, err = c.Do(req, res)

	if err != nil {
		return nil, err
	}

	return res, err
}

func (c *Client) CheckEntityExists(entityType, workspace, name string) (*types.Dataset, error) {
	u := fmt.Sprintf("/workspaces/%v/%v/%v", workspace, entityType, name)

	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	res := new(types.Dataset)
	_, err = c.Do(req, res)

	if err != nil {
		return nil, err
	}

	return res, err
}

func (c *Client) ListEntities(entityType, workspace string) (*types.DataSetList, error) {
	u := fmt.Sprintf("/%v/%v", entityType, workspace)

	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	res := new(types.DataSetList)
	_, err = c.Do(req, res)

	if err != nil {
		return nil, err
	}

	return res, err
}

func (c *Client) GetEntity(entityType, workspace, name string) (*types.Dataset, error) {
	u := fmt.Sprintf("/%v/%v/%v", entityType, workspace, name)

	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	res := new(types.Dataset)
	_, err = c.Do(req, res)

	if err != nil {
		return nil, err
	}

	return res, err
}

func (c *Client) GetVersion(entityType, workspace, name, version string) (*types.Version, error) {
	u := fmt.Sprintf("/%v/%v/%v/versions/%v/get", entityType, workspace, name, version)

	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	res := new(types.Version)
	_, err = c.Do(req, res)

	if err != nil {
		return nil, err
	}

	return res, err
}

func (c *Client) CreateEntity(entityType, workspace, name string) (*types.Dataset, error) {
	u := fmt.Sprintf("/%v/%v/%v", entityType, workspace, name)

	req, err := c.NewRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}
	res := new(types.Dataset)
	_, err = c.Do(req, res)

	if err != nil {
		return nil, err
	}

	return res, err
}

func (c *Client) CreateVersion(entityType, workspace, name, version string) (*types.Version, error) {
	u := fmt.Sprintf("/%v/%v/%v/versions/%v", entityType, workspace, name, version)

	req, err := c.NewRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}
	res := new(types.Version)
	_, err = c.Do(req, res)

	if err != nil {
		return nil, err
	}

	return res, err
}

func (c *Client) ListVersions(entityType, workspace, datasetName string) (*types.VersionList, error) {
	u := fmt.Sprintf("/%v/%v/%v/versions", entityType, workspace, datasetName)

	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	res := new(types.VersionList)
	_, err = c.Do(req, res)

	if err != nil {
		return nil, err
	}

	return res, err
}

func (c *Client) SaveFileStructure(structure types.FileStructure,
	entityType, workspace, name, version string, opts types.SaveOpts) error {
	u := fmt.Sprintf("/%v/%v/%v/%v", entityType, workspace, name, version)
	q := url.Values{}
	q.Set("format", "gobgz")

	if opts.Create {
		q.Set("create", "true")
	}
	if opts.Publish {
		q.Set("publish", "true")
	}
	if opts.Comment != "" {
		q.Set("comment", opts.Comment)
	}
	if opts.Editing {
		q.Set("editing", "true")
	}

	if len(q) > 0 {
		u += "?" + q.Encode()
	}

	buf := bytes.NewBuffer([]byte{})
	w := gzip.NewWriter(buf)
	enc := gob.NewEncoder(w)
	err := enc.Encode(structure)
	if err != nil {
		return nil
	}
	w.Close()

	req, err := c.NewRequest("POST", u, buf)
	if err != nil {
		return err
	}
	_, err = c.Do(req, nil)

	if err != nil {
		return err
	}
	return nil
}

func (c *Client) UploadFile(entityType, workspace, entityName, version, fileName string, body io.ReadCloser) (*types.HashedFile, error) {
	u := fmt.Sprintf(
		"/%v/%v/%v/versions/%v/upload/%v",
		entityType, workspace, entityName, version, fileName,
	)
	u = strings.TrimSuffix(c.BaseURL.String(), "/") + u

	req, err := http.NewRequest("POST", u, body)
	if err != nil {
		return nil, err
	}
	c.setNeededHeaders(req)

	f := types.HashedFile{}
	_, err = c.Do(req, &f)

	return &f, err
}

func (c *Client) DownloadFile(entityType, workspace, entityName, version, fileName string) (io.ReadCloser, error) {
	u := fmt.Sprintf(
		"/%v/%v/%v/versions/%v/raw/%v",
		entityType, workspace, entityName, version, fileName,
	)
	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("[go-plukclient] %v %v", req.Method, req.URL)
	resp, err := c.Client.Do(req)

	if err != nil {
		return nil, err
	}

	resp, err = checkResponse(resp, err)
	if err != nil {
		return nil, err
	}

	return resp.Body, err
}

func (c *Client) DeleteFile(entityType, workspace, entityName, version, fileName string) error {
	u := fmt.Sprintf(
		"/%v/%v/%v/versions/%v/upload/%v",
		entityType, workspace, entityName, version, fileName,
	)
	req, err := c.NewRequest("DELETE", u, nil)
	if err != nil {
		return err
	}
	_, err = c.Do(req, nil)
	return err
}

func (c *Client) CheckChunk(hash string, version byte) (*types.ChunkCheck, error) {
	u := fmt.Sprintf("/chunks/%v/%v", hash, version)

	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	res := new(types.ChunkCheck)
	_, err = c.Do(req, res)

	if err != nil {
		return nil, err
	}

	return res, err
}

func (c *Client) DownloadChunk(hash string, version byte) (io.ReadCloser, error) {
	u := fmt.Sprintf("/chunks/%v/download/%v", hash, version)

	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("[go-plukclient] %v %v", req.Method, req.URL)
	resp, err := c.Client.Do(req)

	if err != nil {
		return nil, err
	}

	resp, err = checkResponse(resp, err)
	if err != nil {
		return nil, err
	}

	return resp.Body, err
}

func (c *Client) GetFSStructure(entityType, workspace, name, version, filter string) (*plukio.ChunkedFileFS, error) {
	u := fmt.Sprintf("/%v/%v/%v/versions/%v/fs", entityType, workspace, name, version)

	query := url.Values{}
	query.Add("format", "gob")
	if filter != "" {
		query.Add("filter", filter)
	}
	u = u + "?" + query.Encode()

	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer([]byte{})
	fs := new(plukio.ChunkedFileFS)
	_, err = c.Do(req, buf)

	if err != nil {
		return nil, err
	}

	dec := gob.NewDecoder(buf)
	err = dec.Decode(fs)
	if err != nil {
		return nil, err
	}

	return fs, nil
}

func (c *Client) SaveChunk(hash string, data []byte, version byte) (*types.ChunkCheck, error) {
	u := fmt.Sprintf("/chunks/%v/%v", hash, version)

	req, err := c.NewRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}
	fakeWriter := &utils.FakeWriter{}
	rd := io.TeeReader(bytes.NewReader(data), fakeWriter)
	req.Body = ioutil.NopCloser(rd)
	res := new(types.ChunkCheck)
	_, err = c.Do(req, res)

	if err != nil {
		return nil, err
	}
	if fakeWriter.Written != len(data) || int64(len(data)) != res.Size {
		return nil, fmt.Errorf(
			"Data length doesn't match the length of written data in the request: "+
				"got %v, need %v", fakeWriter.Written, len(data),
		)
	}

	return nil, err
}

func (c *Client) SaveChunkReader(hash string, reader io.Reader, dataLen int64, version byte) error {
	u := fmt.Sprintf("/chunks/%v/%v", hash, version)

	req, err := c.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}
	req.Body = ioutil.NopCloser(reader)
	res := new(types.ChunkCheck)
	_, err = c.Do(req, res)

	if err != nil {
		return err
	}

	if dataLen != res.Size {
		return fmt.Errorf(
			"Data length doesn't match the length of written data in the request: "+
				"got %v, need %v", res.Size, dataLen,
		)
	}

	return err
}

func (c *Client) acquireWebsocket() *types.WebsocketClient {
	now := time.Now()
	for {
		c.wsLock.Lock()
		for websocketClient, acquired := range c.ws {
			if !acquired && !websocketClient.Closed {
				c.ws[websocketClient] = true
				c.wsLock.Unlock()
				return websocketClient
			}
		}
		c.wsLock.Unlock()
		if time.Since(now) >= time.Minute {
			logrus.Fatalf("Cannot pick up the websocket connections: It seems all the connections are already closed.")
		}
		//time.Sleep(time.Millisecond * 10)
	}
}

func (c *Client) releaseWebsocket(ws *types.WebsocketClient) {
	c.wsLock.Lock()
	c.ws[ws] = false
	c.wsLock.Unlock()
}

func (c *Client) websocketReadWrite(read bool, ws *types.WebsocketClient, msg types.Message) error {
	needRelease := false
	if ws == nil {
		ws = c.acquireWebsocket()
		needRelease = true
	}

	attempts := 5
	libmsg := &libtypes.Message{}
	var err error
	for {
		if attempts == 0 {
			return err
		}
		if read {
			err = ws.Ws.ReadJSON(libmsg)
		} else {
			err = ws.WriteMessage(msg.Type(), msg)
		}
		if err != nil {
			if errC, ok := err.(*websocket.CloseError); ok {
				if errC.Code == websocket.CloseAbnormalClosure {
					ws.Closed = true
					//fmt.Println("Close socket\n")
					c.releaseWebsocket(ws)
					ws = c.acquireWebsocket()
					attempts--
					continue
				}
			} else {
				return err
			}
		}
		break
	}
	if needRelease {
		c.releaseWebsocket(ws)
	}
	if read {
		if err = utils.LoadAsJson(libmsg.Content.(map[string]interface{}), msg); err != nil {
			return err
		}
	}
	return err
}

func (c *Client) writeWebsocketMessage(msg types.Message, ws *types.WebsocketClient) error {
	return c.websocketReadWrite(false, ws, msg)
}

func (c *Client) readWebsocketMessage(ws *types.WebsocketClient, msg types.Message) error {
	return c.websocketReadWrite(true, ws, msg)
}

func (c *Client) SaveChunkWebsocket(hash string, data []byte) error {
	chunkData := &types.ChunkData{Data: data, Hash: hash}
	return c.writeWebsocketMessage(chunkData, nil)
}

func (c *Client) CheckChunkWebsocket(hash string) (*types.ChunkCheck, error) {
	ws := c.acquireWebsocket()
	defer c.releaseWebsocket(ws)

	chunkCheck := &types.ChunkCheck{Hash: hash}
	if err := c.writeWebsocketMessage(chunkCheck, ws); err != nil {
		return nil, err
	}
	if err := c.readWebsocketMessage(ws, chunkCheck); err != nil {
		return nil, err
	}
	return chunkCheck, nil
}

func (c *Client) Close() error {
	for ws := range c.ws {
		ws.Ws.Close()
	}
	return nil
}

func (c *Client) DownloadEntity(entityType, workspace, name, version string, w io.Writer) error {
	u := fmt.Sprintf("/%v/%v/%v/versions/%v", entityType, workspace, name, version)

	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}

	_, err = c.Do(req, w)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) EntityTarSize(entityType, workspace, name, version string) (int64, error) {
	u := fmt.Sprintf("/%v/%v/%v/versions/%v/tarsize", entityType, workspace, name, version)

	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return 0, err
	}

	b := bytes.NewBufferString("")
	_, err = c.Do(req, b)
	if err != nil {
		return 0, err
	}
	out := strings.TrimSuffix(b.String(), "\n")
	return strconv.ParseInt(out, 10, 64)
}

func (c *Client) DeleteEntity(entityType, workspace, name string, force bool) error {
	u := fmt.Sprintf("/%v/%v/%v", entityType, workspace, name)

	if force {
		u = u + "?force=true"
	}

	req, err := c.NewRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	_, err = c.Do(req, nil)
	return err
}

func (c *Client) DeleteVersion(entityType, workspace, name, version string) error {
	u := fmt.Sprintf("/%v/%v/%v/versions/%v", entityType, workspace, name, version)

	req, err := c.NewRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	_, err = c.Do(req, nil)
	return err
}

func (c *Client) WebdavAuth(user, pass, path string) (bool, error) {
	u := path

	req, err := c.NewRequest("OPTIONS", u, nil)
	if err != nil {
		return false, err
	}
	req.URL.Path = u
	req.SetBasicAuth(user, pass)

	_, err = c.Do(req, nil)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Do sends an API request and returns the API response.  The API response is
// JSON decoded and stored in the value pointed to by v, or returned as an
// error if an API error has occurred.  If v implements the io.Writer
// interface, the raw response body will be written to v, without attempting to
// first decode it.
func (c *Client) Do(req *http.Request, v interface{}) (*http.Response, error) {
	logrus.Debugf("[go-plukclient] %v %v", req.Method, req.URL)
	resp, err := c.Client.Do(req)
	if err != nil {
		if e, ok := err.(*url.Error); ok {
			return nil, e
		}
		return nil, err
	}

	defer func() {
		// Drain up to 512 bytes and close the body to let the Transport reuse the connection
		io.CopyN(ioutil.Discard, resp.Body, 512)
		resp.Body.Close()
	}()

	if resp, err = checkResponse(resp, err); err != nil {
		return resp, err
	}
	if v != nil {
		if w, ok := v.(io.Writer); ok {
			io.Copy(w, resp.Body)
		} else {
			err = json.NewDecoder(resp.Body).Decode(v)
			if err == io.EOF {
				err = nil // ignore EOF errors caused by empty response body
			}
		}
	}

	return resp, err
}

func checkResponse(resp *http.Response, err error) (*http.Response, error) {
	if err != nil || resp.StatusCode >= 400 {
		if err != nil {
			return &http.Response{StatusCode: http.StatusInternalServerError}, err
		} else {
			messageBytes, _ := ioutil.ReadAll(resp.Body)
			e := &liberrs.Error{}
			err = json.Unmarshal(messageBytes, e)
			if err != nil {
				message := strconv.Itoa(resp.StatusCode) + ": " + string(messageBytes)
				return resp, errors.New(message)
			} else {
				// Include reason in message if any
				if e.Reason != "" {
					e.Message += "; " + string(e.Reason)
					e.Reason = ""
				}
				return resp, e
			}
		}
	}
	return resp, nil
}
