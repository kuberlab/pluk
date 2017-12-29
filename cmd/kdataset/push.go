package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/cmd/kdataset/config"
	chunk_io "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/spf13/cobra"
)

type pushCmd struct {
	workspace string
	name      string
	version   string
	chunkSize int
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

	return cmd
}

func (cmd *pushCmd) run() error {
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

	checkAndUpload := func(chunkData []byte, hash string) error {
		resp, err := client.CheckChunk(hash)
		if err != nil {
			return err
		}
		if !resp.Exists {
			// Upload chunk.
			if err = client.SaveChunk(hash, chunkData); err != nil {
				return err
			}
		}
		return nil
	}

	logrus.Debug("Run push...")
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
			if uploadError := checkAndUpload(chunkData, hash); uploadError != nil {
				return uploadError
			}
			hashed.Size += uint64(len(chunkData))
			hashed.Hashes = append(hashed.Hashes, hash)
		}
		logrus.Debugf("Whole file size = %v", hashed.Size)
		structure.Files = append(structure.Files, hashed)
		return nil
	})
	if err != nil {
		logrus.Error(err)
		return nil
	}

	// finally, commit file structure.
	logrus.Debugf("File structure: %v", structure)
	if err = client.SaveFileStructure(structure, cmd.workspace, cmd.name, cmd.version); err != nil {
		logrus.Error(err)
		return nil
	}
	logrus.Info("Successfully uploaded.")

	return nil
}
