package api

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/db"
	"github.com/kuberlab/pluk/pkg/gc"
	"github.com/kuberlab/pluk/pkg/utils"
	"github.com/pborman/uuid"
)

var (
	// server is a test HTTP server used to provide mock API responses.
	server *httptest.Server

	// client is needed to make request to the server.
	client *http.Client
)

func runGC() {
	go gc.Start()
}

func getFname() string {
	id := uuid.New()
	fname := filepath.Join("/tmp/", id)
	return fname
}

func setup(fname string) {
	// test server
	db.DbMgr = db.NewFakeDatabaseMgr(fname)
	logrus.SetLevel(logrus.DebugLevel)
	server = httptest.NewServer(GlobalHandler(Build()))
	client = &http.Client{Timeout: time.Second * 10}
	os.Setenv("DATA_DIR", "/tmp/tmp_pluk")
	runGC()
}

// teardown closes the test HTTP server.
func teardown(fname string) {
	server.Close()

	allTables := []string{
		"files",
		"chunks",
		"dataset_versions",
		"datasets",
		"file_chunks",
	}

	for _, t := range allTables {
		db.DbMgr.DB().Exec(fmt.Sprintf("DELETE FROM %v", t))
	}

	db.DbMgr.Close()
	os.RemoveAll("/tmp/tmp_pluk")
	os.Remove(fname)
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
