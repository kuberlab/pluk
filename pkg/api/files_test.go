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
var fileData3 = "dummy content"

func TestUploadSingleFile(t *testing.T) {
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

	var f types.HashedFile
	if err := json.NewDecoder(resp.Body).Decode(&f); err != nil {
		t.Fatal(err)
	}

	utils.Assert("file.txt", f.Path, t)
	utils.Assert(int64(15), f.Size, t)
	utils.Assert(uint32(0644), uint32(f.Mode), t)
	utils.Assert(1, len(f.Hashes), t)
}

func TestUploadFileNotFound(t *testing.T) {
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

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/raw/wrong.txt")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	utils.Assert(http.StatusNotFound, resp.StatusCode, t)
}

func TestDeleteFileNotFound(t *testing.T) {
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

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/upload/wrong.txt")
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusNotFound, resp.StatusCode, t)
}

func TestUploadMultipleFiles(t *testing.T) {
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

	var f types.HashedFile
	if err := json.NewDecoder(resp.Body).Decode(&f); err != nil {
		t.Fatal(err)
	}

	utils.Assert("file2.txt", f.Path, t)
	utils.Assert(int64(30), f.Size, t)
	utils.Assert(uint32(0644), uint32(f.Mode), t)
	utils.Assert(1, len(f.Hashes), t)
}

func TestUploadSameFile(t *testing.T) {
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
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(fileData1))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	var f types.HashedFile
	if err := json.NewDecoder(resp.Body).Decode(&f); err != nil {
		t.Fatal(err)
	}

	utils.Assert("file2.txt", f.Path, t)
	utils.Assert(int64(15), f.Size, t)
	utils.Assert(uint32(0644), uint32(f.Mode), t)
	utils.Assert(1, len(f.Hashes), t)
}

func TestUploadFileWithPrefixRepeated(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	url := buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file11")
	resp, err := client.Post(url, "application/json", bytes.NewBufferString(fileData1))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file1")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(fileData2))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file1")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(fileData2))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/raw/file11")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	data := mustRead(resp.Body)

	utils.Assert(fileData1, data, t)

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []io.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(2, len(fs), t)
}

func TestUploadSameFileDeleteRead(t *testing.T) {
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
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(fileData1))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	var f types.HashedFile
	if err := json.NewDecoder(resp.Body).Decode(&f); err != nil {
		t.Fatal(err)
	}

	// Now, delete file file.txt and check read file2.txt
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file.txt")
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusNoContent, resp.StatusCode, t)

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/raw/file2.txt")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	data := mustRead(resp.Body)

	utils.Assert(fileData1, data, t)
}

func TestUploadHierarchy(t *testing.T) {
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

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []io.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(2, len(fs), t)
	dir := fs[0]
	f := fs[1]

	utils.Assert("folder", dir.Fname, t)
	utils.Assert(true, dir.Dir, t)
	utils.Assert(uint32(0775), uint32(dir.Fmode), t)

	utils.Assert("file1.txt", f.Fname, t)
	utils.Assert(int64(15), f.Fsize, t)
	utils.Assert(uint32(0644), uint32(f.Fmode), t)

	// Go deeper :)
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/tree/folder")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(1, len(fs), t)
	f = fs[0]
	utils.Assert("file2.txt", f.Fname, t)
	utils.Assert(int64(30), f.Fsize, t)
	utils.Assert(uint32(0644), uint32(f.Fmode), t)
}

func TestDeleteDirectory(t *testing.T) {
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

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/upload/folder/file3.txt")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(fileData3))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	// Delete the whole directory
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/upload/folder")
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusNoContent, resp.StatusCode, t)

	// Check the file tree
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/tree")
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

	utils.Assert("file1.txt", f.Fname, t)
	utils.Assert(int64(15), f.Fsize, t)
	utils.Assert(uint32(0644), uint32(f.Fmode), t)
}

func TestUploadReadFile(t *testing.T) {
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

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/raw/file.txt")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	data := mustRead(resp.Body)

	utils.Assert(fileData1, string(data), t)
}

func TestUploadReadTree(t *testing.T) {
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

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/tree")
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

func TestDeleteFile(t *testing.T) {
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

	// Check tree: must be 2 files
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []io.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(2, len(fs), t)

	// Try delete
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file.txt")
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusNoContent, resp.StatusCode, t)

	// Check if file was deleted
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

func TestCloneUploadTheSameFile(t *testing.T) {
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

	// Try upload more
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file2.txt")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(fileData2))
	if err != nil {
		t.Fatal(err)
	}
	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	// Commit!
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/commit")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(""))
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

	// Upload the same name with different content
	url = buildURL("dataset/workspace/dataset/versions/1.0.1/upload/file.txt")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(fileData2))
	if err != nil {
		t.Fatal(err)
	}

	// Check tree: must be 2 entries
	url = buildURL("dataset/workspace/dataset/versions/1.0.1/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []io.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(2, len(fs), t)

	// Check file content: must match fileData2
	url = buildURL("dataset/workspace/dataset/versions/1.0.1/raw/file.txt")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	data := mustRead(resp.Body)

	utils.Assert(fileData2, data, t)
}

func TestCommitNoMoreUpload(t *testing.T) {
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

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/commit")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(""))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusOK, resp.StatusCode, t)

	// Try upload more
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file2.txt")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(fileData2))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusForbidden, resp.StatusCode, t)
}

func TestCommitNoDelete(t *testing.T) {
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

	url = buildURL("dataset/workspace/dataset/versions/1.0.0/commit")
	resp, err = client.Post(url, "application/json", bytes.NewBufferString(""))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusOK, resp.StatusCode, t)

	// Try delete
	url = buildURL("dataset/workspace/dataset/versions/1.0.0/upload/file.txt")
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusForbidden, resp.StatusCode, t)
}
