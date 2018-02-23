package main

import (
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"

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
	debug      bool
)

func initConfig(cmd *cobra.Command, args []string) error {
	initLogging()
	// Expand the path
	path, err := exec.Command("sh", "-c", fmt.Sprintf("echo %v", configPath)).Output()
	if err != nil {
		return err
	}

	overridePlukURL := func() {
		if baseURL != "" {
			config.Config.PlukURL = baseURL
		}
		if config.Config.BaseURL != "" && config.Config.PlukURL == "" {
			u, err := url.Parse(config.Config.BaseURL)
			if err != nil {
				config.Config.PlukURL = defaultBaseURL
			} else {
				config.Config.PlukURL = fmt.Sprintf("%v://%v/pluk/v1", u.Scheme, u.Host)
			}
		}
		if baseURL == "" && config.Config.PlukURL == "" {
			config.Config.PlukURL = defaultBaseURL
		}

	}
	upath := strings.TrimSuffix(string(path), "\n")
	_, err = os.Stat(upath)
	if err != nil && os.IsNotExist(err) {
		logrus.Errorln(err)
		config.Config = &config.DealerConfig{}
		overridePlukURL()
		return nil
	}

	err = config.InitConfig(upath)
	if err != nil {
		return err
	}
	overridePlukURL()

	return nil
}

func initLogging() {
	logrus.SetFormatter(&logrus.TextFormatter{TimestampFormat: "2006-01-02 15:04:05", FullTimestamp: true})

	if debug {
		logLevel = "debug"
	}
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
		Version:           GetVersion().String(),
	}

	p := rootCmd.PersistentFlags()
	// Declare common arguments.
	p.StringVar(&logLevel, "log-level", defaultLogLevel, "Logging level. One of (debug, info, warning, error)")
	p.BoolVarP(&debug, "debug", "", false, "Enable debug level (shortcut for --log-level=debug).")
	p.StringVarP(&configPath, "config", "", defaultConfigPath, "Path to config file")
	p.StringVar(&baseURL, "url", "", "Base url to dataset storage.")

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
