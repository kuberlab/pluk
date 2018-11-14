package plukclient

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/websocket"
	liberrs "github.com/kuberlab/lib/pkg/errors"
	libtypes "github.com/kuberlab/lib/pkg/types"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

type Client struct {
	Client    *http.Client
	BaseURL   *url.URL
	UserAgent string

	auth *AuthOpts
	conn *websocket.Conn
	ws   *types.WebsocketClient
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

func NewClient(baseURL string, auth *AuthOpts) (plukio.PlukClient, error) {
	baseURL = strings.TrimSuffix(baseURL, "/")
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	if len(base.Path) < 2 {
		base.Path = "/pluk/v1"
	}
	// Clone default transport
	var transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
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
	}, nil
}

func (c *Client) PrepareWebsocket() error {
	dialer := websocket.Dialer{}
	urlStr := "/websocket"

	var scheme string
	switch c.BaseURL.Scheme {
	case "http":
		scheme = "ws"
	case "https":
		scheme = "wss"
	}
	u := fmt.Sprintf("%v://%v/%v", scheme, c.BaseURL.Host, strings.TrimPrefix(c.BaseURL.Path, "/"))
	u = strings.TrimSuffix(u, "/") + urlStr
	logrus.Debugf("Connect to %v", u)
	conn, resp, err := dialer.Dial(u, c.authHeaders())

	if err != nil {
		return err
	}
	c.conn = conn
	id := resp.Header.Get("Sec-Websocket-Accept")
	c.ws = types.NewWebsocketClient(conn, id)
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
	entityType, workspace, name, version, comment string, create bool, publish bool) error {
	u := fmt.Sprintf("/%v/%v/%v/%v", entityType, workspace, name, version)
	q := url.Values{}

	if create {
		q.Set("create", "true")
	}
	if publish {
		q.Set("publish", "true")
	}
	if comment != "" {
		q.Set("comment", comment)
	}

	if len(q) > 0 {
		u += "?" + q.Encode()
	}

	req, err := c.NewRequest("POST", u, structure)
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

func (c *Client) CheckChunk(hash string) (*types.ChunkCheck, error) {
	u := fmt.Sprintf("/chunks/%v", hash)

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

func (c *Client) DownloadChunk(hash string) (io.ReadCloser, error) {
	u := fmt.Sprintf("/chunks/%v/download", hash)

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

func (c *Client) GetFSStructure(entityType, workspace, name, version string) (*plukio.ChunkedFileFS, error) {
	u := fmt.Sprintf("/%v/%v/%v/versions/%v/fs", entityType, workspace, name, version)

	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	fs := new(plukio.ChunkedFileFS)
	_, err = c.Do(req, fs)

	if err != nil {
		return nil, err
	}

	return fs, nil
}

func (c *Client) SaveChunk(hash string, data []byte) error {
	u := fmt.Sprintf("/chunks/%v", hash)

	req, err := c.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}
	req.Body = ioutil.NopCloser(bytes.NewReader(data))
	_, err = c.Do(req, nil)

	if err != nil {
		return err
	}

	return err
}

func (c *Client) SaveChunkWebsocket(hash string, data []byte) error {
	chunkData := types.ChunkData{Data: data, Hash: hash}
	return c.ws.WriteMessage(chunkData.Type(), chunkData)
}

func (c *Client) CheckChunkWebsocket(hash string) (*types.ChunkCheck, error) {
	chunkCheck := types.ChunkCheck{Hash: hash}
	err := c.ws.WriteMessage(chunkCheck.Type(), chunkCheck)
	if err != nil {
		return nil, err
	}
	msg := libtypes.Message{}
	if err = c.ws.Ws.ReadJSON(&msg); err != nil {
		return nil, err
	}
	if msg.Type != "chunkCheck" {
		return nil, fmt.Errorf("Wrong message type: %v", msg.Type)
	}
	if err = utils.LoadAsJson(msg.Content.(map[string]interface{}), &chunkCheck); err != nil {
		return nil, err
	}

	return &chunkCheck, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
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
