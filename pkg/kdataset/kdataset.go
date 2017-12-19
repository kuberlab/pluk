package main

import (
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	// TODO: change it to dealer config.
	defaultConfigPath = "~/.kuberlab/pluk"
	defaultLogLevel   = "info"
)

var (
	configPath string
	logLevel   string
)

func initLogging(cmd *cobra.Command, args []string) {
	logrus.SetFormatter(&logrus.TextFormatter{TimestampFormat: "2006-01-02 15:04:05", FullTimestamp: true})

	lvl, err := logrus.ParseLevel(logLevel)
	if err != nil {
		logrus.SetLevel(logrus.InfoLevel)
	} else {
		logrus.SetLevel(lvl)
	}
	return
}

func initConfig(cmd *cobra.Command, args []string) error {

	return nil
}

func newRootCmd() *cobra.Command {
	var rootManageCmd = &cobra.Command{
		Use:               "kdataset",
		Short:             "Management script for datasets",
		PersistentPreRun:  initLogging,
		PersistentPreRunE: initConfig,
	}

	p := rootManageCmd.PersistentFlags()
	// Declare common arguments.
	p.StringVarP(&configPath, "config", "", defaultConfigPath, "Path to config file")
	p.StringVar(&logLevel, "log-level", defaultLogLevel, "Logging level. One of (debug, info, warning, error)")

	// Add all commands
	rootManageCmd.AddCommand(
		newPushCmd(),
	)
	return rootManageCmd
}

func main() {
	cmd := newRootCmd()
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
