package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

var fileData1 = `
one
two
three
`
var fileData2 = `
one line
two line
three line
`

func TestUploadSingleFile(t *testing.T) {
	setup()
	dbPrepare(t)
	defer teardown()

	url := buildURL("datasets/workspace/dataset/versions/1.0.0/upload/file.txt")
	resp, err := client.Post(url, "application/json", bytes.NewBufferString(fileData1))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	var f types.HashedFile
	if err := json.NewDecoder(resp.Body).Decode(&f); err != nil {
		t.Fatal(err)
	}

	utils.Assert("file.txt", f.Path, t)
	utils.Assert(int64(15), f.Size, t)
	utils.Assert(uint32(0644), uint32(f.Mode), t)
	utils.Assert(1, len(f.Hashes), t)
}

func TestUploadMultipleFiles(t *testing.T) {
	setup()
	dbPrepare(t)
	defer teardown()

	url := buildURL("datasets/workspace/dataset/versions/1.0.0/upload/file.txt")
	resp, err := client.Post(url, "application/json", bytes.NewBufferString(fileData1))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("datasets/workspace/dataset/versions/1.0.0/upload/file2.txt")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(fileData2))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	var f types.HashedFile
	if err := json.NewDecoder(resp.Body).Decode(&f); err != nil {
		t.Fatal(err)
	}

	utils.Assert("file2.txt", f.Path, t)
	utils.Assert(int64(30), f.Size, t)
	utils.Assert(uint32(0644), uint32(f.Mode), t)
	utils.Assert(1, len(f.Hashes), t)
}

func TestUploadReadFile(t *testing.T) {
	setup()
	dbPrepare(t)
	defer teardown()

	url := buildURL("datasets/workspace/dataset/versions/1.0.0/upload/file.txt")
	resp, err := client.Post(url, "application/json", bytes.NewBufferString(fileData1))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("datasets/workspace/dataset/versions/1.0.0/raw/file.txt")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	data := mustRead(resp.Body)

	utils.Assert(fileData1, string(data), t)
}

func TestUploadReadTree(t *testing.T) {
	setup()
	dbPrepare(t)
	defer teardown()

	url := buildURL("datasets/workspace/dataset/versions/1.0.0/upload/file.txt")
	resp, err := client.Post(url, "application/json", bytes.NewBufferString(fileData1))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("datasets/workspace/dataset/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []io.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(1, len(fs), t)
	f := fs[0]
	utils.Assert(false, f.Dir, t)
	utils.Assert("file.txt", f.Fname, t)
	utils.Assert(int64(15), f.Fsize, t)
	utils.Assert(uint32(0644), uint32(f.Fmode), t)
}
