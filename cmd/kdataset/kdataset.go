package main

import (
	"fmt"
	"net/url"
	"os"
	"os/user"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/cmd/kdataset/config"
	"github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/plukclient"
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
	insecure   bool
	logLevel   string
	debug      bool
	entityType = &EntityType{Value: defaultEntityType}
)

func initConfig(cmd *cobra.Command, args []string) error {
	initLogging()
	// Expand the path
	path := configPath
	if configPath == defaultConfigPath {
		u, _ := user.Current()
		path = strings.Replace(defaultConfigPath, "~", u.HomeDir, -1)
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
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		logrus.Errorln(err)
		config.Config = &config.DealerConfig{}
		overridePlukURL()
		return nil
	}

	err = config.InitConfig(path)
	if err != nil {
		return err
	}

	// Override workspace & secret if needed.
	ws := os.Getenv("WORKSPACE_NAME")
	secret := os.Getenv("WORKSPACE_SECRET")
	if ws != "" && secret != "" {
		config.Config.Workspace = ws
		config.Config.WorkspaceSecret = secret
	}

	insecureEnv := os.Getenv("KUBERLAB_INSECURE")
	if strings.ToLower(insecureEnv) == "true" {
		config.Config.Insecure = true
	}
	if insecure {
		config.Config.Insecure = true
	}

	overridePlukURL()

	// check new version
	// curl https://api.github.com/repos/kuberlab/pluk/tags

	return nil
}

func initClient() (io.PlukClient, error) {
	return plukclient.NewClient(
		config.Config.PlukURL,
		&plukclient.AuthOpts{
			Token:              config.Config.Token,
			Workspace:          config.Config.Workspace,
			Secret:             config.Config.WorkspaceSecret,
			InsecureSkipVerify: config.Config.Insecure,
		},
	)
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
	p.BoolVarP(&insecure, "insecure", "", false, "Enable insecure SSL/TLS connection (skip verify).")
	p.Var(entityType, "type", fmt.Sprintf("Choose entityType type for request: %v", plukclient.AllowedTypesList()))

	// Add all commands
	rootCmd.AddCommand(
		completionCmd(rootCmd),
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
