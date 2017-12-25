package main

import (
	"errors"
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/cmd/kdataset/config"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/spf13/cobra"
)

type datasetDeleteCmd struct {
	workspace string
	name      string
}

func NewDatasetDeleteCmd() *cobra.Command {
	datasets := &datasetDeleteCmd{}
	cmd := &cobra.Command{
		Use:   "dataset-delete <workspace> <dataset-name>",
		Short: "List datasets for current workspace.",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Validation
			if len(args) < 2 {
				return errors.New("Too few arguments.")
			}
			workspace := args[0]
			name := args[1]

			datasets.name = name
			datasets.workspace = workspace

			return datasets.run()
		},
	}

	return cmd
}

func (cmd *datasetDeleteCmd) run() (err error) {
	client, err := plukclient.NewClient(
		config.Config.PlukURL,
		&plukclient.AuthOpts{Token: config.Config.Token},
	)
	if err != nil {
		return err
	}

	logrus.Debug("Run dataset-delete...")

	err = client.DeleteDataset(cmd.workspace, cmd.name)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}

	logrus.Infof("Dataset %v successfully deleted.")
	return
}
