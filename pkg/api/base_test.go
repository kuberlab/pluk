package api

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/db"
	"github.com/kuberlab/pluk/pkg/utils"
)

var (
	// server is a test HTTP server used to provide mock API responses.
	server *httptest.Server

	// client is needed to make request to the server.
	client *http.Client
)

func setup() {
	// test server
	db.DbMgr = db.NewFakeDatabaseMgr()
	logrus.SetLevel(logrus.DebugLevel)
	server = httptest.NewServer(GlobalHandler())
	client = &http.Client{Timeout: time.Second * 10}
	os.Setenv("DATA_DIR", "/tmp/tmp_pluk")
}

// teardown closes the test HTTP server.
func teardown() {
	server.Close()
	db.DbMgr.Close()
	os.RemoveAll("/tmp/tmp_pluk")
}

func buildURL(urlStr string) string {
	strings.TrimPrefix(urlStr, "/")
	return fmt.Sprintf("%v%v/%v", server.URL, utils.ApiPrefix, urlStr)
}

func mustRead(r io.ReadCloser) string {
	data, _ := ioutil.ReadAll(r)
	r.Close()
	return string(data)
}
