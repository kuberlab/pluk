package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/sirupsen/logrus"
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
		Use:   "version-list <workspace> <entity-name>",
		Short: "List versions for the given catalog entity.",
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
	client, err := initClient()
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Debug("Run version-list...")

	versions, err := client.ListVersions(entityType.Value, cmd.workspace, cmd.name)
	if err != nil {
		logrus.Fatal(err)
	}

	w := tabwriter.NewWriter(os.Stdout, 5, 4, 3, ' ', 0)
	_, _ = fmt.Fprintln(w, "VERSION\tSIZE\tCREATED\tUPDATED")
	for _, v := range versions.Versions {
		columns := []string{
			v.Version,
			sizeString(v.SizeBytes),
			v.CreatedAt.String(),
			v.UpdatedAt.String(),
		}
		_, _ = fmt.Fprintln(w, strings.Join(columns, "\t"))
	}
	_ = w.Flush()

	return nil
}

func sizeString(size int64) string {
	suffix := "b"
	sz := float64(size)
	if size > 1024 {
		sz = sz / 1024.0
		suffix = "K"
	} else {
		return fmt.Sprintf("%.0f%v", sz, suffix)
	}
	if size > 1024*1024 {
		sz = sz / 1024.0
		suffix = "M"
	}
	if size > 1024*1024*1024 {
		sz = sz / 1024.0
		suffix = "G"
	}
	return fmt.Sprintf("%.3f%v", sz, suffix)
}
