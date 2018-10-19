package main

import (
	"errors"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
)

type versionDeleteCmd struct {
	workspace string
	name      string
	version   string
	dsType    string
}

func NewVersionDeleteCmd() *cobra.Command {
	deleteV := &versionDeleteCmd{}
	cmd := &cobra.Command{
		Use:   "version-delete <workspace> <dataset-name>:<version>",
		Short: "Delete specific version of the dataset.",
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

			deleteV.name = nameVersion[0]
			deleteV.version = nameVersion[1]
			deleteV.workspace = workspace

			return deleteV.run()
		},
	}
	f := cmd.Flags()
	f.StringVarP(
		&deleteV.dsType,
		"type",
		"",
		"dataset",
		"dataset type",
	)

	return cmd
}

func (cmd *versionDeleteCmd) run() (err error) {
	client, err := initClient()
	if err != nil {
		return err
	}

	logrus.Debug("Run version-delete...")

	err = client.DeleteVersion(cmd.dsType, cmd.workspace, cmd.name, cmd.version)
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Infof("Version %v of dataset %v successfully deleted.", cmd.version, cmd.name)
	return
}
