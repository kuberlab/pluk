package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"gopkg.in/cheggaaa/pb.v1"

	"github.com/Sirupsen/logrus"
	chunk_io "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/kuberlab/pluk/pkg/utils"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"
)

type pushCmd struct {
	chunkSize   int
	concurrency int64
	dsType      string
	name        string
	version     string
	workspace   string
	create      bool
	force       bool
	publish     bool
	websocket   bool
}

func NewPushCmd() *cobra.Command {
	push := &pushCmd{}
	cmd := &cobra.Command{
		Use:   "push <workspace> <dataset-name>:<version>",
		Short: "Push the dataset within current directory",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Validation
			if len(args) < 2 {
				return errors.New("Too few arguments.")
			}
			workspace := args[0]
			nameVersion := strings.Split(args[1], ":")
			if len(nameVersion) != 2 {
				return errors.New("Dataset and version is invalid. Must be in form <dataset-name>:<version>")
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
		512000,
		"Chunk-size for scanning",
	)
	f.Int64VarP(
		&push.concurrency,
		"concurrency",
		"c",
		int64(runtime.NumCPU()),
		"Number of concurrent request to server.",
	)
	f.StringVarP(
		&push.dsType,
		"type",
		"",
		"dataset",
		"dataset type",
	)
	f.BoolVarP(
		&push.create,
		"create",
		"",
		false,
		"Create dataset in cloud-dealer if not exists.",
	)
	f.BoolVarP(
		&push.publish,
		"publish",
		"",
		false,
		"Newly created dataset will be public. Only used in conjunction with --create.",
	)
	f.BoolVarP(
		&push.force,
		"force",
		"f",
		false,
		"Force dataset uploading regardless warnings.",
	)
	f.BoolVarP(
		&push.websocket,
		"websocket",
		"w",
		false,
		"Use websocket for connecting to server. Decreases the number of requests.",
	)

	return cmd
}

func (cmd *pushCmd) run() error {
	logrus.Debugf("Concurrency is set to %v.", cmd.concurrency)
	cwd, err := os.Getwd()
	if err != nil {
		logrus.Fatal(err)
	}

	client, err := initClient()
	if err != nil {
		logrus.Fatal(err)
	}

	if err = utils.CheckVersion(cmd.version); err != nil {
		logrus.Fatal(err)
	}

	if _, err := client.CheckWorkspace(cmd.workspace); err != nil && !cmd.force {
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "not found") {
			logrus.Fatalf("Probably workspace '%v' doesn't exist. Check if workspace name is right.", cmd.workspace)
		} else {
			logrus.Fatal(err)
		}
		return nil
	}

	if _, err := client.CheckEntity(cmd.dsType, cmd.workspace, cmd.name); err != nil && !cmd.create && !cmd.force {
		if strings.Contains(err.Error(), "not found") {
			logrus.Fatalf("Dataset '%v' doesn't exist. Consider using --create option to automatically create dataset or use --force.", cmd.name)
		} else {
			logrus.Fatal(err)
		}
		return nil
	}

	if cmd.websocket {
		if err = client.PrepareWebsocket(); err != nil {
			logrus.Fatal(err)
		}
	}
	defer client.Close()

	structure := types.FileStructure{Files: make([]*types.HashedFile, 0)}

	logrus.Debug("Run push...")
	var totalSize int64 = 0

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
		totalSize += f.Size()
		return nil
	})

	var sem *semaphore.Weighted
	if cmd.websocket {
		sem = semaphore.NewWeighted(1)
	} else {
		sem = semaphore.NewWeighted(cmd.concurrency)
	}
	lock := &sync.RWMutex{}
	ctx := context.TODO()
	bar := pb.New64(totalSize).SetUnits(pb.U_BYTES)
	bar.SetMaxWidth(100)
	bar.ShowSpeed = true
	bar.Start()

	var resp *types.ChunkCheck
	checkAndUpload := func(chunkData []byte, hash string) error {
		defer func() {
			lock.Lock()
			bar.Add(len(chunkData))
			lock.Unlock()
			sem.Release(1)
		}()

		if cmd.websocket {
			resp, err = client.CheckChunkWebsocket(hash)
		} else {
			resp, err = client.CheckChunk(hash)
		}
		if err != nil {
			logrus.Fatalf("Failed to check chunk: %v", err)
		}
		if !resp.Exists || resp.Size != int64(len(chunkData)) {
			// Upload chunk.
			if cmd.websocket {
				if err = client.SaveChunkWebsocket(hash, chunkData); err != nil {
					logrus.Errorf("Failed to upload chunk: %v", err)
					os.Exit(1)
				}
			} else {
				if err = client.SaveChunk(hash, chunkData); err != nil {
					logrus.Fatalf("Failed to upload chunk: %v", err)
				}
			}
		}
		return nil
	}

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
		hashed := &types.HashedFile{Path: strings.TrimPrefix(path, cwd+"/")}
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
			hashed.Hashes = append(hashed.Hashes, types.Hash{Hash: hash, Size: length})

		}
		file.Close()
		logrus.Debugf("Whole file size = %v", hashed.Size)
		structure.Files = append(structure.Files, hashed)
		return nil
	})

	// Wait for all.
	if cmd.websocket {
		sem.Acquire(ctx, 1)
	} else {
		sem.Acquire(ctx, cmd.concurrency)
	}

	if err != nil {
		bar.Finish()
		logrus.Fatal(err)
	}

	// finally, commit file structure.
	logrus.Debugf("File structure: %v", structure)
	if err = client.SaveFileStructure(
		structure,
		cmd.dsType,
		cmd.workspace,
		cmd.name,
		cmd.version,
		cmd.create,
		cmd.publish,
	); err != nil {
		bar.Finish()
		logrus.Fatal(err)
	}
	if !bar.IsFinished() {
		bar.Finish()
	}
	logrus.Info("Successfully uploaded.")

	return nil
}
