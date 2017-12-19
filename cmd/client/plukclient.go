package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/api"
	"github.com/kuberlab/pluk/pkg/dataset"
)

type Client struct {
	Client    *http.Client
	BaseURL   *url.URL
	UserAgent string
}

func NewClient(baseURL string) (*Client, error) {
	baseURL = strings.TrimSuffix(baseURL, "/")
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	baseClient := &http.Client{Timeout: time.Minute}
	return &Client{BaseURL: base, Client: baseClient, UserAgent: "go-plukclient/1"}, nil
}

func (c *Client) NewRequest(method, urlStr string, body interface{}) (*http.Request, error) {
	u := c.BaseURL.String()
	u = strings.TrimSuffix(u, "/") + urlStr

	var buf io.ReadWriter
	if body != nil {
		buf = new(bytes.Buffer)
		err := json.NewEncoder(buf).Encode(body)
		if err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest(method, u, buf)
	if err != nil {
		return nil, err
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	return req, nil
}

func (c *Client) CheckChunk(hash string) (*api.CheckChunkResponse, error) {
	u := fmt.Sprintf("%v/%v", "/chunks", hash)

	req, err := c.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	res := new(api.CheckChunkResponse)
	_, err = c.Do(req, res)

	if err != nil {
		return nil, err
	}

	return res, err
}

func (c *Client) CommitFileStructure(structure dataset.FileStructure, workspace, name, version string) error {
	u := fmt.Sprintf("/datasets/%v/%v/%v", workspace, name, version)

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

func (c *Client) SaveChunk(hash string, data []byte) error {
	u := fmt.Sprintf("%v/chunks/%v", c.BaseURL, hash)

	req, err := http.NewRequest("POST", u, bytes.NewReader(data))
	if err != nil {
		return err
	}
	_, err = c.Do(req, nil)

	if err != nil {
		return err
	}

	return err
}

// Do sends an API request and returns the API response.  The API response is
// JSON decoded and stored in the value pointed to by v, or returned as an
// error if an API error has occurred.  If v implements the io.Writer
// interface, the raw response body will be written to v, without attempting to
// first decode it.
func (c *Client) Do(req *http.Request, v interface{}) (*http.Response, error) {
	logrus.Debugf("[go-client] %v %v", req.Method, req.URL)
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
			message := strconv.Itoa(resp.StatusCode) + ": " + string(messageBytes)
			return resp, errors.New(message)
		}
	}
	return resp, nil
}
