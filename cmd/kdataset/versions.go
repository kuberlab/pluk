package main

import (
	"errors"
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/cmd/kdataset/config"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/spf13/cobra"
)

type versionsCmd struct {
	workspace string
	name      string
	version   string
	output    string
}

func NewVersionsCmd() *cobra.Command {
	versions := &versionsCmd{}
	cmd := &cobra.Command{
		Use:   "version-list <workspace> <dataset-name>",
		Short: "List versions for the current dataset.",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Validation
			if len(args) < 2 {
				return errors.New("Too few arguments.")
			}
			workspace := args[0]
			name := args[1]

			versions.workspace = workspace
			versions.name = name

			return versions.run()
		},
	}

	return cmd
}

func (cmd *versionsCmd) run() error {
	client, err := plukclient.NewClient(
		config.Config.PlukURL,
		&plukclient.AuthOpts{Token: config.Config.Token},
	)
	if err != nil {
		logrus.Error(err)
		return nil
	}

	logrus.Debug("Run version-list...")

	versions, err := client.ListVersions(cmd.workspace, cmd.name)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}

	fmt.Println("VERSIONS:")
	for _, v := range versions.Versions {
		fmt.Println(v)
	}

	return nil
}
