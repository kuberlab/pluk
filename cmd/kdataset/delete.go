package main

import (
	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
)

type datasetDeleteCmd struct {
	workspace string
	name      string
	force     bool
}

func NewDatasetDeleteCmd() *cobra.Command {
	datasets := &datasetDeleteCmd{}
	cmd := &cobra.Command{
		Use:   "delete <workspace> <dataset-name>",
		Short: "Delete specific catalog entity.",
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
	f := cmd.Flags()
	f.BoolVarP(
		&datasets.force,
		"force",
		"f",
		false,
		"Delete entity immediately (run garbage collector).",
	)

	return cmd
}

func (cmd *datasetDeleteCmd) run() (err error) {
	client, err := initClient()
	if err != nil {
		return err
	}

	logrus.Debug("Run delete...")

	err = client.DeleteEntity(entityType.Value, cmd.workspace, cmd.name, cmd.force)
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Infof("%v %v successfully deleted.", entityType.Value, cmd.name)
	return
}
