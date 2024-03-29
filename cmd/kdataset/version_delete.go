package main

import (
	"errors"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type versionDeleteCmd struct {
	workspace string
	name      string
	version   string
}

func NewVersionDeleteCmd() *cobra.Command {
	deleteV := &versionDeleteCmd{}
	cmd := &cobra.Command{
		Use:   "version-delete <workspace> <entity-name>:<version>",
		Short: "Delete specific version of the catalog entity.",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Validation
			if len(args) < 2 {
				return errors.New("Too few arguments.")
			}
			workspace := args[0]
			nameVersion := strings.Split(args[1], ":")
			if len(nameVersion) != 2 {
				return errors.New("Entity name and version is invalid. Must be in form <entity-name>:<version>")
			}

			deleteV.name = nameVersion[0]
			deleteV.version = nameVersion[1]
			deleteV.workspace = workspace

			return deleteV.run()
		},
	}

	return cmd
}

func (cmd *versionDeleteCmd) run() (err error) {
	client, err := initClient()
	if err != nil {
		return err
	}

	logrus.Debug("Run version-delete...")

	err = client.DeleteVersion(entityType.Value, cmd.workspace, cmd.name, cmd.version)
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Infof(
		"Version %v of %v %v successfully deleted.",
		cmd.version, entityType.Value, cmd.name,
	)
	return
}
