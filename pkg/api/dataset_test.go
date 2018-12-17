package api

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/db"
	plukio "github.com/kuberlab/pluk/pkg/io"
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
	want := db.Dataset{
		Type:      "dataset",
		Workspace: "workspace",
		Name:      "new-test",
		ID:        2,
		Deleted:   false,
	}
	utils.Assert(want.Type, dataset.Type, t)
	utils.Assert(want.Workspace, dataset.Workspace, t)
	utils.Assert(want.Name, dataset.Name, t)
	utils.Assert(want.Deleted, dataset.Deleted, t)
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

	var version types.Version
	if err := json.NewDecoder(resp.Body).Decode(&version); err != nil {
		t.Fatal(err)
	}
	want := types.Version{
		Type:      "dataset",
		SizeBytes: 0,
		Version:   "1.0.0-new",
		Editing:   true,
	}
	utils.Assert(want.Type, version.Type, t)
	utils.Assert(want.SizeBytes, version.SizeBytes, t)
	utils.Assert(want.Version, version.Version, t)
	utils.Assert(want.Editing, version.Editing, t)
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
	var fs []plukio.ChunkedFileInfo
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

func TestForkDataset(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	url := buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file1.txt")
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

	// Commit!
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/commit")
	resp, err = client.Post(url, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusOK, resp.StatusCode, t)

	url = buildURL("dataset/workspace/dataset/fork/another-ws")
	resp, err = client.Post(url, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/another-ws/dataset/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []plukio.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(2, len(fs), t)
}

func TestForkDatasetName(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	url := buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file1.txt")
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

	// Commit!
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/commit")
	resp, err = client.Post(url, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusOK, resp.StatusCode, t)

	url = buildURL("dataset/workspace/dataset/fork/another-ws?name=new-dataset")
	resp, err = client.Post(url, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/another-ws/new-dataset/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []plukio.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(2, len(fs), t)
}

func TestForkDatasetNameType(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	url := buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file1.txt")
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

	// Commit!
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/commit")
	resp, err = client.Post(url, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusOK, resp.StatusCode, t)

	url = buildURL("dataset/workspace/dataset/fork/another-ws?name=new-model&type=model")
	resp, err = client.Post(url, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("model/another-ws/new-model/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []plukio.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(2, len(fs), t)
}

func TestForkDatasetForce(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	_ = db.DbMgr.CreateDataset(&db.Dataset{
		Name:      "dataset",
		Workspace: "another-ws",
		Type:      "dataset",
		Deleted:   false,
	})

	url := buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file1.txt")
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

	// Commit!
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/commit")
	resp, err = client.Post(url, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusOK, resp.StatusCode, t)

	// Fork
	url = buildURL("dataset/workspace/dataset/fork/another-ws?force=true")
	resp, err = client.Post(url, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/another-ws/dataset/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []plukio.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(2, len(fs), t)
}

func TestDownloadDataset(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	url := buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file1.txt")
	resp, err := client.Post(url, "application/json", bytes.NewBufferString(fileData1))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/upload/folder/file2.txt")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(fileData2))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	// Commit!
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/commit")
	resp, err = client.Post(url, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusOK, resp.StatusCode, t)

	// Download
	url = buildURL("dataset/workspace/dataset/versions/1.0.0")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusOK, resp.StatusCode, t)

	reader := tar.NewReader(resp.Body)
	var files int = 0
	for {
		hd, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}

		switch hd.Typeflag {
		case tar.TypeReg, tar.TypeRegA:
			files++
		}
		_, _ = reader.Read(make([]byte, hd.Size))
	}

	utils.Assert(2, files, t)
}

func TestUploadCorrectChunk(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	//hash1 := utils.CalcHash([]byte(fileData1))
	hash2 := utils.CalcHash([]byte(fileData2))

	// Post chunk1 by hash2 (simulate corrupted data)
	url := buildURL("chunks/" + hash2)
	resp, err := client.Post(url, "application/json", bytes.NewBufferString(fileData1))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	// Check chunk2 by hash2 (to upload correct data)
	url = buildURL("chunks/" + hash2)
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}

	var chunk types.ChunkCheck
	if err := json.NewDecoder(resp.Body).Decode(&chunk); err != nil {
		t.Fatal(err)
	}

	utils.Assert(true, chunk.Exists, t)
	utils.Assert(int64(len(fileData1)), chunk.Size, t)
	utils.Assert(hash2, chunk.Hash, t)
	utils.Assert(true, chunk.Size != int64(len(fileData2)), t)

	// Upload correct data
	// Post chunk1 by hash2 (simulate corrupted data)
	url = buildURL("chunks/" + hash2)
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(fileData2))
	if err != nil {
		t.Fatal(err)
	}

	// Check chunk2 by hash2
	url = buildURL("chunks/" + hash2)
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}

	if err := json.NewDecoder(resp.Body).Decode(&chunk); err != nil {
		t.Fatal(err)
	}

	utils.Assert(int64(len(fileData2)), chunk.Size, t)
	utils.Assert(true, chunk.Exists, t)
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
