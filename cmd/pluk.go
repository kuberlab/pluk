package main

import (
	"os"
	"github.com/spf13/cobra"
	"github.com/kuberlab/pluk/cmd/push"
	"github.com/kuberlab/pluk/cmd/logging"
)

const (
	// TODO: change it to dealer config.
	defaultConfigPath = "~/.kuberlab/pluk"
	defaultLogLevel   = "debug"
)

var (
	configPath string
	baseURL    string
)


func initConfig(cmd *cobra.Command, args []string) error {
	return nil
}

func newRootCmd() *cobra.Command {
	var rootCmd = &cobra.Command{
		Use:               "kdataset",
		Short:             "Management script for datasets",
		PersistentPreRun:  logging.InitLogging,
		PersistentPreRunE: initConfig,
	}

	p := rootCmd.PersistentFlags()
	// Declare common arguments.
	p.StringVar(&logging.LogLevel, "log-level", defaultLogLevel, "Logging level. One of (debug, info, warning, error)")
	p.StringVarP(&configPath, "config", "", defaultConfigPath, "Path to config file")
	p.StringVar(&baseURL, "url", "", "Base url to pluk.")

	// Add all commands
	rootCmd.AddCommand(
		push.NewPushCmd(),
	)
	return rootCmd
}

func main() {
	cmd := newRootCmd()
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
