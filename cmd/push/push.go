package push

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"io"
	chunk_io "github.com/kuberlab/pluk/pkg/io"
	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/dataset"
	"github.com/spf13/cobra"
	"github.com/kuberlab/pluk/cmd/logging"
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
		Use:    "push <workspace> <dataset-name>:<version>",
		Short:  "Push the dataset within current directory",
		PreRun: logging.InitLogging,
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
		r := chunk_io.NewChunkedReader(cmd.chunkSize, file)
		// Populate file structure.
		hashed := &dataset.HashedFile{Path: strings.TrimPrefix(path, cwd+"/")}
		var chunkData []byte
		var hash string
		var err error
		for err != io.EOF {
			chunkData, hash, err = r.NextChunk()
			if err != nil && err != io.EOF {
				return err
			} else if len(chunkData) > 0 {
				if uploadError := checkAndUpload(chunkData, hash); uploadError != nil {
					return uploadError
				}
				hashed.Hashes = append(hashed.Hashes, hash)
			}
		}
		structure.Files = append(structure.Files, hashed)
		return nil
	})
	if err != nil {
		return err
	}

	// finally, commit file structure.
	logrus.Debugf("File structure: %v", structure)
	if err = client.CommitFileStructure(structure, cmd.workspace, cmd.name, cmd.version); err != nil {
		return err
	}
	logrus.Info("Successfully uploaded.")

	return
}
