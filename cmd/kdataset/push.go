package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	chunk_io "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"
	"gopkg.in/cheggaaa/pb.v1"
)

type pushCmd struct {
	chunkSize   int
	concurrency int64
	name        string
	version     string
	workspace   string
	comment     string
	specFile    string
	create      bool
	force       bool
	publish     bool
	skipUpload  bool
	//websocket   bool
}

func NewPushCmd() *cobra.Command {
	push := &pushCmd{}
	cmd := &cobra.Command{
		Use:   "push <workspace> <entity-name>:<version>",
		Short: "Push the data within the current directory",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Validation
			if len(args) < 2 {
				return errors.New("Too few arguments.")
			}
			workspace := args[0]
			nameVersion := strings.Split(args[1], ":")
			if len(nameVersion) != 2 {
				return fmt.Errorf(
					"%v and version are invalid. Must be in form <%v-name>:<version>",
					strings.Title(entityType.Value), entityType.Value,
				)
			}

			push.workspace = workspace
			push.name = nameVersion[0]
			push.version = nameVersion[1]

			return push.run()
		},
	}

	f := cmd.Flags()
	f.IntVarP(
		&push.chunkSize,
		"chunk-size",
		"",
		1024000,
		"Chunk-size for scanning",
	)
	f.StringVar(
		&push.comment,
		"comment",
		"",
		"Comment for the new version",
	)
	f.StringVar(
		&push.specFile,
		"spec",
		"",
		"Spec file for the model",
	)
	f.Int64VarP(
		&push.concurrency,
		"concurrency",
		"c",
		int64(runtime.NumCPU()),
		"Number of concurrent request to server.",
	)
	f.BoolVarP(
		&push.create,
		"create",
		"",
		false,
		"Create entity in catalog if not exists.",
	)
	f.BoolVarP(
		&push.skipUpload,
		"skip-upload",
		"",
		false,
		"Skip upload chunks and move right to committing FS",
	)
	f.BoolVarP(
		&push.publish,
		"publish",
		"",
		false,
		"Newly created catalog entity will be public. Only used in conjunction with --create.",
	)
	f.BoolVarP(
		&push.force,
		"force",
		"f",
		false,
		"Force uploading regardless warnings.",
	)
	//f.BoolVarP(
	//	&push.websocket,
	//	"websocket",
	//	"w",
	//	false,
	//	"Use websocket for connecting to server. Decreases the number of requests.",
	//)

	return cmd
}

func (cmd *pushCmd) run() error {
	logrus.Debugf("Concurrency is set to %v.", cmd.concurrency)
	cwd, err := os.Getwd()
	if err != nil {
		logrus.Fatal(err)
	}

	var specData *bytes.Buffer
	if cmd.specFile != "" {
		// Only model allow spec
		if entityType.Value != "model" {
			logrus.Fatal("Only model is allowed to have --spec")
		}
		specRaw, err := ioutil.ReadFile(cmd.specFile)
		if err != nil {
			logrus.Fatalf("Failed to read %v: %v", cmd.specFile, err)
		}
		specData = bytes.NewBuffer(specRaw)
	}

	client, err := initClient()
	if err != nil {
		logrus.Fatal(err)
	}

	if err = utils.CheckVersion(cmd.version); err != nil {
		logrus.Fatal(err)
	}

	// Even with force, we must check the access to the given workspace.
	if _, err := client.CheckWorkspace(cmd.workspace); err != nil {
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "not found") {
			logrus.Fatalf("Probably workspace '%v' doesn't exist. Check if workspace name is right.", cmd.workspace)
		} else if strings.Contains(err.Error(), "Forbidden to manage item") {
			logrus.Fatalf("You don't have write %v permission to the given workspace: %q.", entityType, cmd.workspace)
		} else {
			logrus.Fatal(err)
		}
		return nil
	}

	if _, err := client.CheckEntityPermission(entityType.Value, cmd.workspace, cmd.name, true); err != nil {
		if strings.Contains(err.Error(), "Forbidden to manage item") {
			logrus.Fatalf("You don't have write %v permission to the given workspace: %q.", entityType, cmd.workspace)
		} else {
			logrus.Fatal(err)
		}
	}

	if _, err = client.CheckEntityExists(entityType.Value, cmd.workspace, cmd.name); err != nil {
		if strings.Contains(err.Error(), "not found") {
			// Only skip if doesn't exist
			if !cmd.force && !cmd.create {
				logrus.Fatalf(
					"%v '%v' doesn't exist. Consider using --create option to "+
						"automatically create dataset or use --force.",
					strings.Title(entityType.Value), cmd.name,
				)
			}
		}
	}

	//if cmd.websocket {
	//	if err = client.PrepareWebsocket(); err != nil {
	//		logrus.Fatal(err)
	//	}
	//}
	defer client.Close()

	logrus.Debug("Run push...")
	var totalSize int64 = 0
	var fileCount int64 = 0

	// Populate all files size.
	err = filepath.Walk(cwd, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(f.Name(), ".") {
			return nil
		}
		if f.IsDir() {
			return nil
		}
		for _, part := range strings.Split(strings.TrimPrefix(path, cwd), "/") {
			if strings.HasPrefix(part, ".") {
				return nil
			}
		}
		totalSize += f.Size()
		fileCount++
		return nil
	})

	// Bar bytes
	bar := pb.New64(totalSize).SetUnits(pb.U_BYTES).SetMaxWidth(100)
	bar.ShowSpeed = true

	// Bar files
	barFiles := pb.New64(fileCount).SetUnits(pb.U_NO).SetMaxWidth(100)
	barFiles.ShowSpeed = true

	pool, err := pb.StartPool(bar, barFiles)
	pool.RefreshRate = 150 * time.Millisecond

	bufLimit := 5000
	fileChan := make(chan *types.HashedFile, 10000)

	fileBuf := make([]*types.HashedFile, 0)

	flushBuf := func(last bool) {
		structure := types.FileStructure{Files: fileBuf}
		if err = client.SaveFileStructure(
			structure,
			entityType.Value,
			cmd.workspace,
			cmd.name,
			cmd.version,
			types.SaveOpts{
				Comment: cmd.comment,
				Publish: cmd.publish,
				Create:  cmd.create,
				Editing: !last,
			},
		); err != nil {
			logrus.Fatal(err)
		}
	}

	syncCh := make(chan bool, 0)
	uploadFS := func() {
		for f := range fileChan {
			if f == nil {
				break
			}
			if len(fileBuf) >= bufLimit {
				flushBuf(false)
				fileBuf = nil
			}
			fileBuf = append(fileBuf, f)
		}
		syncCh <- true
	}

	go uploadFS()
	err = cmd.uploadChunks(bar, barFiles, client, !cmd.skipUpload, fileChan)
	close(fileChan)
	_ = pool.Stop()

	// finally, commit file structure.
	logrus.Info("Committing FS structure...")
	// Wait for emptying fileChan
	<-syncCh
	flushBuf(true)
	//logrus.Debugf("File structure: %v", structure)

	//if err = client.SaveFileStructure(
	//	structure,
	//	entityType.Value,
	//	cmd.workspace,
	//	cmd.name,
	//	cmd.version,
	//	cmd.comment,
	//	cmd.create,
	//	cmd.publish,
	//); err != nil {
	//	logrus.Fatal(err)
	//}

	if cmd.specFile != "" {
		err = client.PostEntitySpec(entityType.Value, cmd.workspace, cmd.name, specData)
		if err != nil {
			logrus.Fatal(err)
		}
	}

	logrus.Info("Successfully uploaded and committed.")

	return nil
}

func (cmd *pushCmd) uploadChunks(
	bar, barFiles *pb.ProgressBar, client chunk_io.PlukClient,
	upload bool, fileChan chan *types.HashedFile) (err error) {
	cwd, err := os.Getwd()
	if err != nil {
		logrus.Fatal(err)
	}

	var sem *semaphore.Weighted
	//if cmd.websocket {
	//	sem = semaphore.NewWeighted(1)
	//} else {
	sem = semaphore.NewWeighted(cmd.concurrency)
	//}
	//lock := &sync.RWMutex{}
	ctx := context.TODO()

	var resp *types.ChunkCheck
	checkAndUpload := func(chunkData []byte, hash string) {
		defer func() {
			//lock.Lock()
			//bar.Add(len(chunkData))
			//lock.Unlock()
			sem.Release(1)
		}()

		if !upload {
			bar.Add(len(chunkData))
			return
		}

		rd := bytes.NewReader(chunkData)
		chReader := io.TeeReader(rd, bar)

		//if cmd.websocket {
		//	resp, err = client.CheckChunkWebsocket(hash)
		//} else {
		resp, err = client.CheckChunk(hash, 1)
		//}
		if err != nil {
			logrus.Fatalf("Failed to check chunk: %v", err)
		}
		if !resp.Exists || resp.Size != int64(len(chunkData)) {
			// Upload chunk.
			//if cmd.websocket {
			//	if err = client.SaveChunkWebsocket(hash, chunkData); err != nil {
			//		logrus.Errorf("Failed to upload chunk: %v", err)
			//		os.Exit(1)
			//	}
			//} else {
			if err = client.SaveChunkReader(hash, chReader, 1); err != nil {
				logrus.Fatalf("Failed to upload chunk: %v", err)
			}
			//}
		} else {
			bar.Add(len(chunkData))
		}
		return
	}

	logrus.Infof("Computing files count and estimate directory space...")
	err = filepath.Walk(cwd, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(f.Name(), ".") {
			return nil
		}
		if f.IsDir() {
			return nil
		}
		for _, part := range strings.Split(strings.TrimPrefix(path, cwd), "/") {
			if strings.HasPrefix(part, ".") {
				return nil
			}
		}
		logrus.Debugf("processing %v...", path)

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		r := chunk_io.NewChunkedReader(cmd.chunkSize, file)
		// Populate file structure.
		hashed := &types.HashedFile{
			Path:     strings.TrimPrefix(path, cwd+"/"),
			Mode:     f.Mode(),
			ModeTime: f.ModTime(),
		}
		var chunkData []byte
		var hash string
		for {
			chunkData, hash, err = r.NextChunk()
			if err != nil && err != io.EOF {
				return err
			}

			//logrus.Debugf("chunk len: %v", len(chunkData))
			if len(chunkData) == 0 {
				break
			}

			sem.Acquire(ctx, 1)
			go checkAndUpload(chunkData, hash)

			length := int64(len(chunkData))
			hashed.Size += length
			hashed.Hashes = append(hashed.Hashes, types.Hash{Hash: hash, Size: length, Version: 1})

		}
		file.Close()
		barFiles.Increment()
		logrus.Debugf("Whole file size = %v", hashed.Size)
		//files = append(files, hashed)
		fileChan <- hashed
		return nil
	})

	// Wait for all.
	//if cmd.websocket {
	//	sem.Acquire(ctx, 1)
	//} else {
	sem.Acquire(ctx, cmd.concurrency)
	//}

	if err != nil {
		bar.Finish()
		logrus.Fatal(err)
	}

	if !bar.IsFinished() {
		bar.Finish()
	}
	return nil
}
