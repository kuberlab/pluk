package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/db"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

func TestListDatasets(t *testing.T) {
	setup()
	dbPrepare(t)
	defer teardown()

	resp, err := client.Get(buildURL("datasets/workspace"))
	if err != nil {
		t.Fatal(err)
	}

	var datasets types.DataSetList
	if err := json.NewDecoder(resp.Body).Decode(&datasets); err != nil {
		t.Fatal(err)
	}
	want := types.DataSetList{
		Datasets: []types.Dataset{types.Dataset{Workspace: "workspace", Name: "dataset"}},
	}
	utils.Assert(want, datasets, t)
}

func TestCreateDataset(t *testing.T) {
	setup()
	dbPrepare(t)
	defer teardown()

	url := buildURL("datasets/workspace/new-test")
	resp, err := client.Post(url, "application/json", bytes.NewBufferString(""))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	var dataset db.Dataset
	if err := json.NewDecoder(resp.Body).Decode(&dataset); err != nil {
		t.Fatal(err)
	}
	want := db.Dataset{Workspace: "workspace", Name: "new-test", ID: 2, Deleted: false}
	utils.Assert(want, dataset, t)
}

func TestCreateVersionAuto(t *testing.T) {
	setup()
	dbPrepare(t)
	defer teardown()

	url := buildURL("datasets/workspace/new-test/versions/1.0.0-new")
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
		Workspace: "workspace",
		Name:      "new-test",
		Version:   "1.0.0-new",
		ID:        2,
		Deleted:   false,
		Editing:   true}
	utils.Assert(want, dsv, t)
}

func TestCreateVersionWrong(t *testing.T) {
	setup()
	dbPrepare(t)
	defer teardown()

	url := buildURL("datasets/workspace/new-test/versions/new")
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
	if err := db.DbMgr.CreateDataset(&db.Dataset{Workspace: "workspace", Name: "dataset"}); err != nil {
		t.Fatal(err)
	}

	if err := db.DbMgr.CreateDatasetVersion(
		&db.DatasetVersion{
			Workspace: "workspace",
			Name:      "dataset",
			Version:   "1.0.0",
		}); err != nil {
		t.Fatal(err)
	}
}
