package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"io"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/dataset"
	"github.com/kuberlab/pluk/pkg/types"
	"github.com/spf13/cobra"
)

type pushCmd struct {
	workspace string
	name      string
	version   string
	chunkSize int
}

func newPushCmd() *cobra.Command {
	push := &pushCmd{}
	cmd := &cobra.Command{
		Use:    "push <workspace> <dataset-name>:<version>",
		Short:  "Push the dataset within current directory",
		PreRun: initLogging,
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
		1048576,
		"Chunk-size for scanning",
	)

	return cmd
}

func (cmd *pushCmd) run() (err error) {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	client, err := NewClient(baseURL)
	if err != nil {
		return err
	}

	structure := dataset.FileStructure{Files: make([]*dataset.HashedFile, 0)}

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
		r := types.NewChunkedReader(cmd.chunkSize, file)
		// Populate file structure.
		hashed := &dataset.HashedFile{Path: strings.TrimPrefix(path, cwd+"/")}

		for {
			chunkData, hash, err := r.NextChunk()
			if err != nil {
				if err == io.EOF {
					// Last
					if err = checkAndUpload(chunkData, hash); err != nil {
						return err
					}
					hashed.Hashes = append(hashed.Hashes, hash)
					break
				} else {
					return err
				}
			}
			if err = checkAndUpload(chunkData, hash); err != nil {
				return err
			}
			hashed.Hashes = append(hashed.Hashes, hash)
		}
		structure.Files = append(structure.Files, hashed)
		return nil
	})
	if err != nil {
		return err
	}

	// finally, commit file structure.

	return
}
