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
	"github.com/kuberlab/pluk/cmd/kdataset/config"
	chunk_io "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"
)

type pushCmd struct {
	workspace   string
	name        string
	version     string
	chunkSize   int
	concurrency int64
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
		10485760,
		"Chunk-size for scanning",
	)
	f.Int64VarP(
		&push.concurrency,
		"concurrency",
		"c",
		int64(runtime.NumCPU()),
		"Number of concurrent request to server.",
	)

	return cmd
}

func (cmd *pushCmd) run() error {
	logrus.Debugf("Concurrency is set to %v.", cmd.concurrency)
	cwd, err := os.Getwd()
	if err != nil {
		logrus.Error(err)
		return nil
	}

	client, err := plukclient.NewClient(
		config.Config.PlukURL,
		&plukclient.AuthOpts{Token: config.Config.Token},
	)
	if err != nil {
		logrus.Error(err)
		return nil
	}

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

	sem := semaphore.NewWeighted(cmd.concurrency)
	lock := &sync.RWMutex{}
	ctx := context.TODO()
	bar := pb.New64(totalSize).SetUnits(pb.U_BYTES)
	bar.SetMaxWidth(100)
	bar.ShowSpeed = true
	bar.Start()

	checkAndUpload := func(chunkData []byte, hash string) error {
		defer func() {
			lock.Lock()
			bar.Add(len(chunkData))
			lock.Unlock()
			sem.Release(1)
		}()
		resp, err := client.CheckChunk(hash)
		if err != nil {
			logrus.Errorf("Failed to check chunk: %v", err)
			os.Exit(1)
		}
		if !resp.Exists {
			// Upload chunk.
			if err = client.SaveChunk(hash, chunkData); err != nil {
				logrus.Errorf("Failed to upload chunk: %v", err)
				os.Exit(1)
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
			if err == io.EOF {
				break
			}
			logrus.Debugf("chunk len: %v", len(chunkData))
			if len(chunkData) == 0 {
				break
			}

			sem.Acquire(ctx, 1)
			go checkAndUpload(chunkData, hash)

			hashed.Size += uint64(len(chunkData))
			hashed.Hashes = append(hashed.Hashes, hash)

		}
		logrus.Debugf("Whole file size = %v", hashed.Size)
		structure.Files = append(structure.Files, hashed)
		return nil
	})

	// Wait for all.
	sem.Acquire(ctx, cmd.concurrency)

	if err != nil {
		bar.Finish()
		logrus.Error(err)
		return nil
	}

	// finally, commit file structure.
	logrus.Debugf("File structure: %v", structure)
	if err = client.SaveFileStructure(structure, cmd.workspace, cmd.name, cmd.version); err != nil {
		bar.Finish()
		logrus.Error(err)
		return nil
	}
	if !bar.IsFinished() {
		bar.Finish()
	}
	logrus.Info("Successfully uploaded.")

	return nil
}
