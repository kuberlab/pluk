package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/db"
	"github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

func TestListDatasets(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	resp, err := client.Get(buildURL("dataset/workspace"))
	if err != nil {
		t.Fatal(err)
	}

	var datasets types.DataSetList
	if err := json.NewDecoder(resp.Body).Decode(&datasets); err != nil {
		t.Fatal(err)
	}
	want := types.DataSetList{
		Items: []types.Dataset{types.Dataset{Type: "dataset", Workspace: "workspace", Name: "dataset"}},
	}
	utils.Assert(want, datasets, t)
}

func TestCreateDataset(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	url := buildURL("dataset/workspace/new-test")
	resp, err := client.Post(url, "application/json", bytes.NewBufferString(""))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	var dataset db.Dataset
	if err := json.NewDecoder(resp.Body).Decode(&dataset); err != nil {
		t.Fatal(err)
	}
	want := db.Dataset{Type: "dataset", Workspace: "workspace", Name: "new-test", ID: 2, Deleted: false}
	utils.Assert(want, dataset, t)
}

func TestCreateVersionAuto(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	url := buildURL("dataset/workspace/new-test/versions/1.0.0-new")
	resp, err := client.Post(url, "application/json", bytes.NewBufferString(""))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	var dsv db.DatasetVersion
	if err := json.NewDecoder(resp.Body).Decode(&dsv); err != nil {
		t.Fatal(err)
	}
	want := db.DatasetVersion{
		Type:      "dataset",
		Workspace: "workspace",
		Name:      "new-test",
		Version:   "1.0.0-new",
		ID:        2,
		Deleted:   false,
		Editing:   true,
	}
	utils.Assert(want, dsv, t)
}

func TestUploadDeleteCheckDataset(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	url := buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file.txt")
	resp, err := client.Post(url, "application/json", bytes.NewBufferString(fileData1))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file2.txt")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(fileData2))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	// Delete dataset version
	url = buildURL("dataset/workspace/dataset/versions/1.0.0")
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusNoContent, resp.StatusCode, t)

	//time.Sleep(time.Millisecond * 500)

	// Create version again
	url = buildURL("dataset/workspace/dataset/versions/1.0.0")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(""))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []io.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(0, len(fs), t)

	// Upload new file
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file3.txt")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(fileData2))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(1, len(fs), t)
}

func TestCreateVersionWrong(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	url := buildURL("dataset/workspace/new-test/versions/new")
	resp, err := client.Post(url, "application/json", bytes.NewBufferString(""))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusBadRequest, resp.StatusCode, t)

	var e errors.Error
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatal(err)
	}
	utils.Assert(
		"new: Invalid Semantic Version; version examples: 1.0.1, 1.5.0-dev, 1.8.1-alpha.1",
		e.Message,
		t,
	)
}

func dbPrepare(t *testing.T) {
	if err := db.DbMgr.CreateDataset(
		&db.Dataset{
			Workspace: "workspace",
			Name:      "dataset",
			Type:      "dataset",
		}); err != nil {
		t.Fatal(err)
	}

	if err := db.DbMgr.CreateDatasetVersion(
		&db.DatasetVersion{
			Workspace: "workspace",
			Name:      "dataset",
			Version:   "1.0.0",
			Editing:   true,
			Type:      "dataset",
		}); err != nil {
		t.Fatal(err)
	}
}
