package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
)

type datasetsCmd struct {
	workspace string
}

func NewDatasetsCmd() *cobra.Command {
	datasets := &datasetsCmd{}
	cmd := &cobra.Command{
		Use:   "list <workspace>",
		Short: "List catalog entities for the given workspace.",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Validation
			if len(args) < 1 {
				return errors.New("Too few arguments.")
			}
			workspace := args[0]

			datasets.workspace = workspace

			return datasets.run()
		},
	}

	return cmd
}

func (cmd *datasetsCmd) run() (err error) {
	client, err := initClient()
	if err != nil {
		return err
	}

	logrus.Debug("Run list...")

	datasets, err := client.ListEntities(entityType.Value, cmd.workspace)
	if err != nil {
		logrus.Fatal(err)
	}

	if len(datasets.Items) == 0 {
		fmt.Printf("No %vs.\n", entityType.Value)
		return nil
	}

	fmt.Printf("%v:\n", strings.ToUpper(entityType.Value))
	for _, ds := range datasets.Items {
		fmt.Println(ds.Name)
	}
	return
}
