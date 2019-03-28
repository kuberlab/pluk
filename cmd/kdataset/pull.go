package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/cheggaaa/pb.v1"

	"io"

	"github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
)

type pullCmd struct {
	workspace string
	name      string
	version   string
	output    string
}

func NewPullCmd() *cobra.Command {
	pull := &pullCmd{}
	cmd := &cobra.Command{
		Use:   "pull <workspace> <entity-name>:<version> [-O output-file.tar]",
		Short: "Download the data entity archive.",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Validation
			if len(args) < 2 {
				return errors.New("Too few arguments.")
			}
			workspace := args[0]
			nameVersion := strings.Split(args[1], ":")
			if len(nameVersion) != 2 {
				return fmt.Errorf(
					"%v and version is invalid. Must be in form <%v-name>:<version>",
					entityType.Value, entityType.Value,
				)
			}

			pull.workspace = workspace
			pull.name = nameVersion[0]
			pull.version = nameVersion[1]

			if pull.output == "" {
				pull.output = fmt.Sprintf("%v-%v.%v.tar", workspace, pull.name, pull.version)
			}

			return pull.run()
		},
	}

	f := cmd.Flags()
	f.StringVarP(
		&pull.output,
		"output",
		"O",
		"",
		"Output filename",
	)

	return cmd
}

func (cmd *pullCmd) run() (err error) {
	client, err := initClient()
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Debug("Run pull...")

	size, err := client.EntityTarSize(entityType.Value, cmd.workspace, cmd.name, cmd.version)
	if err != nil {
		logrus.Fatal(err)
	}
	logrus.Debugf("Tar archive size = %v", size)

	f, err := os.Create(cmd.output)
	if err != nil {
		logrus.Fatal(err)
	}
	defer f.Close()

	bar := pb.New64(size).SetUnits(pb.U_BYTES)
	w := io.MultiWriter(f, bar)

	bar.SetMaxWidth(100)
	bar.ShowSpeed = true
	bar.Start()

	err = client.DownloadEntity(entityType.Value, cmd.workspace, cmd.name, cmd.version, w)
	if err != nil {
		if bar.Get() == 0 {
			os.Remove(cmd.output)
		}
		bar.Finish()
		logrus.Fatal(err)
	}
	bar.Finish()

	logrus.Infof("Successfully downloaded %v to %v.", entityType.Value, cmd.output)
	return
}
