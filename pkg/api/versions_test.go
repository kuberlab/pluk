package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/utils"
)

func TestCloneManyFiles(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	fileNum := 270
	for i := 0; i < fileNum; i++ {
		url := buildURL(fmt.Sprintf("dataset/workspace/dataset/versions/1.0.0/upload/file%v.txt", i))
		resp, err := client.Post(url, "application/json", bytes.NewBufferString(fmt.Sprintf("test%v test%v", i, i)))
		if err != nil {
			t.Fatal(err)
		}

		utils.Assert(http.StatusCreated, resp.StatusCode, t)
	}

	// Commit!
	url := buildURL("dataset/workspace/dataset/versions/1.0.0/commit")
	resp, err := client.Post(url, "application/json", bytes.NewBufferString(""))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusOK, resp.StatusCode, t)

	// Clone
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/clone/1.0.1")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(fileData1))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	// Check tree: must be 300 entries
	url = buildURL("dataset/workspace/dataset/versions/1.0.1/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []io.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(fileNum, len(fs), t)

	// Check file content 1.0.0
	for i := 0; i < fileNum; i++ {
		url = buildURL(fmt.Sprintf("dataset/workspace/dataset/versions/1.0.0/raw/file%v.txt", i))
		resp, err = client.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		data := mustRead(resp.Body)

		utils.Assert(fmt.Sprintf("test%v test%v", i, i), data, t)
	}

	// Check file content 1.0.1
	for i := 0; i < fileNum; i++ {
		url = buildURL(fmt.Sprintf("dataset/workspace/dataset/versions/1.0.1/raw/file%v.txt", i))
		resp, err = client.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		data := mustRead(resp.Body)

		utils.Assert(fmt.Sprintf("test%v test%v", i, i), data, t)
	}
}
