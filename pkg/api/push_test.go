package api

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
)

func TestPushSameChunk(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	chunkHash := utils.CalcHash([]byte(fileData1))

	// Upload chunk
	url := buildURL(fmt.Sprintf("chunks/%v", chunkHash))
	resp, err := client.Post(url, "application/json", bytes.NewBufferString(fileData1))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	// Build file structure
	structure := &types.FileStructure{
		Files: []*types.HashedFile{
			{
				Size:     int64(len(fileData1)),
				Path:     "file1.txt",
				Mode:     0644,
				Hashes:   []types.Hash{{Hash: chunkHash, Size: int64(len(fileData1))}},
				ModeTime: time.Now().Add(-time.Hour),
			},
			{
				Size:     int64(len(fileData1)),
				Path:     "file2.txt",
				Mode:     0644,
				Hashes:   []types.Hash{{Hash: chunkHash, Size: int64(len(fileData1))}},
				ModeTime: time.Now().Add(-time.Hour),
			},
		},
	}

	data, _ := json.Marshal(structure)
	url = buildURL("dataset/workspace/new/1.0.0")
	resp, err = client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/workspace/new/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []plukio.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(2, len(fs), t)

	// Check file content 1.0.0
	for i := 1; i < 3; i++ {
		url = buildURL(fmt.Sprintf("dataset/workspace/new/versions/1.0.0/raw/file%v.txt", i))
		resp, err = client.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		data := mustRead(resp.Body)

		utils.Assert(fileData1, data, t)
	}
}

func TestPushFewFiles(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	// Build file structure
	structure := &types.FileStructure{Files: make([]*types.HashedFile, 0)}
	fileNum := 5
	for i := 0; i < fileNum; i++ {
		data := fmt.Sprintf("test%v test%v", i, i)
		hash := utils.CalcHash([]byte(data))
		// Upload chunk
		url := buildURL(fmt.Sprintf("chunks/%v", hash))
		resp, err := client.Post(url, "application/json", bytes.NewBufferString(data))
		if err != nil {
			t.Fatal(err)
		}

		utils.Assert(http.StatusCreated, resp.StatusCode, t)
		hashed := &types.HashedFile{
			Size:     int64(len(data)),
			Path:     fmt.Sprintf("file%v.txt", i),
			Mode:     0644,
			Hashes:   []types.Hash{{Hash: hash, Size: int64(len(data))}},
			ModeTime: time.Now().Add(-time.Hour),
		}
		structure.Files = append(structure.Files, hashed)
	}

	data, _ := json.Marshal(structure)
	url := buildURL("dataset/workspace/new/1.0.0")
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/workspace/new/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []plukio.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(fileNum, len(fs), t)

	// Check file content 1.0.0
	for i := 0; i < fileNum; i++ {
		url = buildURL(fmt.Sprintf("dataset/workspace/new/versions/1.0.0/raw/file%v.txt", i))
		resp, err = client.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		data := mustRead(resp.Body)
		want := fmt.Sprintf("test%v test%v", i, i)
		utils.Assert(want, data, t)
	}
}

func TestPushSameChunksOldNewAccessible(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	// Build file structure
	structure := &types.FileStructure{Files: make([]*types.HashedFile, 0)}
	fileNum := 5
	for i := 0; i < fileNum; i++ {
		data := fmt.Sprintf("test%v test%v", i, i)
		hash := utils.CalcHash([]byte(data))
		// Upload chunk
		url := buildURL(fmt.Sprintf("chunks/%v", hash))
		resp, err := client.Post(url, "application/json", bytes.NewBufferString(data))
		if err != nil {
			t.Fatal(err)
		}

		utils.Assert(http.StatusCreated, resp.StatusCode, t)
		hashed := &types.HashedFile{
			Size:     int64(len(data)),
			Path:     fmt.Sprintf("file%v.txt", i),
			Mode:     0644,
			Hashes:   []types.Hash{{Hash: hash, Size: int64(len(data))}},
			ModeTime: time.Now().Add(-time.Hour),
		}
		structure.Files = append(structure.Files, hashed)
	}

	data, _ := json.Marshal(structure)
	url := buildURL("dataset/workspace/new/1.0.0")
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	// Build file structure for new chunks version
	structure = &types.FileStructure{Files: make([]*types.HashedFile, 0)}
	for i := 0; i < fileNum; i++ {
		data := fmt.Sprintf("test%v test%v", i, i)
		hash := utils.CalcHash([]byte(data))
		// Upload chunk
		url := buildURL(fmt.Sprintf("chunks/%v/1", hash))
		resp, err := client.Post(url, "application/json", bytes.NewBufferString(data))
		if err != nil {
			t.Fatal(err)
		}

		utils.Assert(http.StatusCreated, resp.StatusCode, t)
		hashed := &types.HashedFile{
			Size:     int64(len(data)),
			Path:     fmt.Sprintf("file%v.txt", i),
			Mode:     0644,
			Hashes:   []types.Hash{{Hash: hash, Size: int64(len(data))}},
			ModeTime: time.Now().Add(-time.Hour),
		}
		structure.Files = append(structure.Files, hashed)
	}

	data, _ = json.Marshal(structure)
	url = buildURL("dataset/workspace/new/1.0.0")
	resp, err = client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/workspace/new/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []plukio.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(fileNum, len(fs), t)

	// Check file content 1.0.0
	for i := 0; i < fileNum; i++ {
		url = buildURL(fmt.Sprintf("dataset/workspace/new/versions/1.0.0/raw/file%v.txt", i))
		resp, err = client.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		data := mustRead(resp.Body)
		want := fmt.Sprintf("test%v test%v", i, i)
		utils.Assert(want, data, t)
	}
}

func TestPushSameChunksNewOldAccessible(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	// Build file structure for new chunks version
	structure := &types.FileStructure{Files: make([]*types.HashedFile, 0)}
	fileNum := 5
	for i := 0; i < fileNum; i++ {
		data := fmt.Sprintf("test%v test%v", i, i)
		hash := utils.CalcHash([]byte(data))
		// Upload chunk
		url := buildURL(fmt.Sprintf("chunks/%v/1", hash))
		resp, err := client.Post(url, "application/json", bytes.NewBufferString(data))
		if err != nil {
			t.Fatal(err)
		}

		utils.Assert(http.StatusCreated, resp.StatusCode, t)
		hashed := &types.HashedFile{
			Size:     int64(len(data)),
			Path:     fmt.Sprintf("file%v.txt", i),
			Mode:     0644,
			Hashes:   []types.Hash{{Hash: hash, Size: int64(len(data))}},
			ModeTime: time.Now().Add(-time.Hour),
		}
		structure.Files = append(structure.Files, hashed)
	}

	data, _ := json.Marshal(structure)
	url := buildURL("dataset/workspace/new/1.0.0")
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	// Build file structure
	structure = &types.FileStructure{Files: make([]*types.HashedFile, 0)}
	for i := 0; i < fileNum; i++ {
		data := fmt.Sprintf("test%v test%v", i, i)
		hash := utils.CalcHash([]byte(data))
		// Upload chunk
		url := buildURL(fmt.Sprintf("chunks/%v", hash))
		resp, err := client.Post(url, "application/json", bytes.NewBufferString(data))
		if err != nil {
			t.Fatal(err)
		}

		utils.Assert(http.StatusCreated, resp.StatusCode, t)
		hashed := &types.HashedFile{
			Size:     int64(len(data)),
			Path:     fmt.Sprintf("file%v.txt", i),
			Mode:     0644,
			Hashes:   []types.Hash{{Hash: hash, Size: int64(len(data))}},
			ModeTime: time.Now().Add(-time.Hour),
		}
		structure.Files = append(structure.Files, hashed)
	}

	data, _ = json.Marshal(structure)
	url = buildURL("dataset/workspace/new/1.0.0")
	resp, err = client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/workspace/new/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []plukio.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(fileNum, len(fs), t)

	// Check file content 1.0.0
	for i := 0; i < fileNum; i++ {
		url = buildURL(fmt.Sprintf("dataset/workspace/new/versions/1.0.0/raw/file%v.txt", i))
		resp, err = client.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		data := mustRead(resp.Body)
		want := fmt.Sprintf("test%v test%v", i, i)
		utils.Assert(want, data, t)
	}
}

func TestPushManyFilesVersion1(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	// Build file structure
	structure := &types.FileStructure{Files: make([]*types.HashedFile, 0)}
	fileNum := 100
	for i := 0; i < fileNum; i++ {
		data := fmt.Sprintf("test%v test%v", i, i)
		hash := utils.CalcHash([]byte(data))
		// Upload chunk
		url := buildURL(fmt.Sprintf("chunks/%v/1", hash))
		resp, err := client.Post(url, "application/json", bytes.NewBufferString(data))
		if err != nil {
			t.Fatal(err)
		}

		utils.Assert(http.StatusCreated, resp.StatusCode, t)
		hashed := &types.HashedFile{
			Size:     int64(len(data)),
			Path:     fmt.Sprintf("file%v.txt", i),
			Mode:     0644,
			Hashes:   []types.Hash{{Hash: hash, Size: int64(len(data)), Version: 1}},
			ModeTime: time.Now().Add(-time.Hour),
		}
		structure.Files = append(structure.Files, hashed)
	}

	data, _ := json.Marshal(structure)
	url := buildURL("dataset/workspace/new/1.0.0")
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/workspace/new/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []plukio.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(fileNum, len(fs), t)

	// Check file content 1.0.0
	for i := 0; i < fileNum; i++ {
		url = buildURL(fmt.Sprintf("dataset/workspace/new/versions/1.0.0/raw/file%v.txt", i))
		resp, err = client.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		data := mustRead(resp.Body)
		want := fmt.Sprintf("test%v test%v", i, i)
		utils.Assert(want, data, t)
	}
}

func TestPushManyFiles2(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	// Build file structure
	structure := &types.FileStructure{Files: make([]*types.HashedFile, 0)}
	fileNum := 100
	for i := 0; i < fileNum; i++ {
		data := fmt.Sprintf("test%v test%v", i, i)
		hash := utils.CalcHash([]byte(data))
		// Upload chunk
		url := buildURL(fmt.Sprintf("chunks/%v", hash))
		resp, err := client.Post(url, "application/json", bytes.NewBufferString(data))
		if err != nil {
			t.Fatal(err)
		}

		utils.Assert(http.StatusCreated, resp.StatusCode, t)
		hashed := &types.HashedFile{
			Size:     int64(len(data)),
			Path:     fmt.Sprintf("file%v.txt", i),
			Mode:     0644,
			Hashes:   []types.Hash{{Hash: hash, Size: int64(len(data))}},
			ModeTime: time.Now().Add(-time.Hour),
		}
		structure.Files = append(structure.Files, hashed)
	}

	data, _ := json.Marshal(structure)
	url := buildURL("dataset/workspace/new/1.0.0")
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/workspace/new/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []plukio.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(fileNum, len(fs), t)

	// Check file content 1.0.0
	for i := 0; i < fileNum; i++ {
		url = buildURL(fmt.Sprintf("dataset/workspace/new/versions/1.0.0/raw/file%v.txt", i))
		resp, err = client.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		data := mustRead(resp.Body)

		utils.Assert(fmt.Sprintf("test%v test%v", i, i), data, t)
	}
}

func TestPushFileRepeatedChunk(t *testing.T) {
	fname := getFname()
	setup(fname)
	dbPrepare(t)
	defer teardown(fname)

	// Build file structure
	structure := &types.FileStructure{Files: make([]*types.HashedFile, 0)}
	fileNum := 5
	for i := 0; i < fileNum; i++ {
		hashes := make([]types.Hash, 0)

		data := strings.Repeat(fmt.Sprintf("%v", i), 3000000)

		rd := plukio.NewChunkedReader(1024000, bytes.NewBufferString(data))
		for {
			chunkData, hash, err := rd.NextChunk()

			if len(chunkData) == 0 {
				break
			}
			// Upload chunk
			url := buildURL(fmt.Sprintf("chunks/%v", hash))
			resp, err := client.Post(url, "application/json", bytes.NewBuffer(chunkData))
			if err != nil {
				t.Fatal(err)
			}

			hashes = append(hashes, types.Hash{Hash: hash, Size: int64(len(chunkData))})

			utils.Assert(http.StatusCreated, resp.StatusCode, t)
		}
		hashed := &types.HashedFile{
			Size:     int64(len(data)),
			Path:     fmt.Sprintf("file%v.txt", i),
			Mode:     0644,
			Hashes:   hashes,
			ModeTime: time.Now().Add(-time.Hour),
		}
		structure.Files = append(structure.Files, hashed)
	}

	data, _ := json.Marshal(structure)
	url := buildURL("dataset/workspace/new/1.0.0")
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(http.StatusCreated, resp.StatusCode, t)

	url = buildURL("dataset/workspace/new/versions/1.0.0/tree")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var fs []plukio.ChunkedFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fs); err != nil {
		t.Fatal(err)
	}

	utils.Assert(fileNum, len(fs), t)

	// Check file content 1.0.0
	for i := 0; i < fileNum; i++ {
		url = buildURL(fmt.Sprintf("dataset/workspace/new/versions/1.0.0/raw/file%v.txt", i))
		resp, err = client.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		data := mustRead(resp.Body)

		utils.Assert(int64(len(data)), resp.ContentLength, t)
	}
}
