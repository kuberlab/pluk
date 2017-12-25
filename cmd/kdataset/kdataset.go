package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/cmd/kdataset/config"
	"github.com/spf13/cobra"
)

const (
	defaultConfigPath = "~/.kuberlab/config"
	defaultBaseURL    = "http://localhost:8082/internal"
	defaultLogLevel   = "info"
)

var (
	configPath string
	baseURL    string
	logLevel   string
)

func initConfig(cmd *cobra.Command, args []string) error {
	initLogging()
	// Expand the path
	path, err := exec.Command("sh", "-c", fmt.Sprintf("echo -n %v", configPath)).Output()
	if err != nil {
		return err
	}

	overridePlukURL := func() {
		if baseURL != "" {
			config.Config.PlukURL = baseURL
		}
		if baseURL == "" && config.Config.PlukURL == "" {
			config.Config.PlukURL = defaultBaseURL
		}
	}

	_, err = os.Stat(string(path))
	if err != nil && os.IsNotExist(err) {
		logrus.Error(err)
		config.Config = &config.DealerConfig{}
		overridePlukURL()
		return nil
	}

	err = config.InitConfig(string(path))
	if err != nil {
		return err
	}
	overridePlukURL()

	return nil
}

func initLogging() {
	logrus.SetFormatter(&logrus.TextFormatter{TimestampFormat: "2006-01-02 15:04:05", FullTimestamp: true})

	lvl, err := logrus.ParseLevel(logLevel)
	if err != nil {
		logrus.SetLevel(logrus.DebugLevel)
	} else {
		logrus.SetLevel(lvl)
	}
	return
}

func newRootCmd() *cobra.Command {
	var rootCmd = &cobra.Command{
		Use:               "kdataset",
		Short:             "Management script for datasets.",
		PersistentPreRunE: initConfig,
	}

	p := rootCmd.PersistentFlags()
	// Declare common arguments.
	p.StringVar(&logLevel, "log-level", defaultLogLevel, "Logging level. One of (debug, info, warning, error)")
	p.StringVarP(&configPath, "config", "", defaultConfigPath, "Path to config file")
	p.StringVar(&baseURL, "url", "", "Base url to pluk.")

	// Add all commands
	rootCmd.AddCommand(
		NewPushCmd(),
		NewPullCmd(),
		NewDatasetsCmd(),
		NewVersionsCmd(),
		NewDatasetDeleteCmd(),
		NewVersionDeleteCmd(),
	)
	return rootCmd
}

func main() {
	cmd := newRootCmd()
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
