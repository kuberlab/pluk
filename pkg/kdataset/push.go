package main

import (
	"errors"

	"strings"

	"github.com/spf13/cobra"
)

type pushCmd struct {
	workspace string
	name      string
	version   string
}

func newPushCmd() *cobra.Command {
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

	return cmd
}

func (cmd *pushCmd) run() (err error) {

	return
}
