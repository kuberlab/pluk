package main

import (
	"fmt"
	"net/url"
	"os"
	"os/user"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/cmd/kdataset/config"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/spf13/cobra"
	"go.uber.org/automaxprocs/maxprocs"
)

const (
	defaultConfigPath = "~/.kuberlab/config"
	defaultBaseURL    = "http://localhost:8082/internal"
	defaultLogLevel   = "info"
)

var (
	configPath      string
	baseURL         string
	oldPlukURL      string
	plukURL         string
	logLevel        string
	token           string
	workspace       string
	workspaceSecret string
	internalKey     string
	entityType      = &EntityType{Value: defaultEntityType}
	insecure        bool
	debug           bool
)

func overridePlukURL() {
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

func initConfig(cmd *cobra.Command, args []string) error {
	initLogging()
	// Expand the path
	path := configPath
	config.InitConfigField(&path, configPath, "KUBERLAB_CONFIG", defaultConfigPath)
	if strings.Contains(path, "~") {
		u, _ := user.Current()
		path = strings.Replace(defaultConfigPath, "~", u.HomeDir, -1)
	}

	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		logrus.Errorln(err)
		// Initialize empty config
		config.Config = &config.DealerConfig{}
	} else {
		err = config.InitConfig(path)
		if err != nil {
			return err
		}
	}

	config.InitConfigField(&config.Config.Token, token, "KUBERLAB_TOKEN", "")

	// Override workspace & secret if needed.
	// But only if token is not provided
	if config.Config.Token == "" {
		config.InitConfigField(&config.Config.Workspace, workspace, "WORKSPACE_NAME", "")
		config.InitConfigField(
			&config.Config.WorkspaceSecret,
			workspaceSecret,
			"WORKSPACE_SECRET",
			"",
		)
	}

	insecureEnv := os.Getenv("KUBERLAB_INSECURE")
	if strings.ToLower(insecureEnv) == "true" || insecure {
		config.Config.Insecure = true
	}

	// Special rule for base pluke url
	baseUrlSource := config.InitConfigField(&config.Config.BaseURL, baseURL, "KUBERLAB_URL", "")
	plukUrlSource := config.InitConfigField(&config.Config.PlukURL, plukURL, "PLUKE_URL", "")
	config.InitConfigField(&config.Config.PlukURL, oldPlukURL, "PLUKE_URL", "")
	config.InitConfigField(&config.Config.InternalKey, internalKey, "INTERNAL_KEY", "")

	if config.Config.BaseURL != "" && baseUrlSource > plukUrlSource {
		// Set priority to baseURL if specified; clear Pluke URL in this case.
		config.Config.PlukURL = ""
	}

	overridePlukURL()

	// check new version
	// curl https://api.github.com/repos/kuberlab/pluk/tags | jq .[0].name

	return nil
}

func initClient() (*plukclient.Client, error) {
	var opts *plukclient.AuthOpts
	if config.Config.InternalKey != "" {
		opts = &plukclient.AuthOpts{
			InternalKey:        config.Config.InternalKey,
			InsecureSkipVerify: config.Config.Insecure,
		}
	} else {
		opts = &plukclient.AuthOpts{
			Token:              config.Config.Token,
			Workspace:          config.Config.Workspace,
			Secret:             config.Config.WorkspaceSecret,
			InsecureSkipVerify: config.Config.Insecure,
		}
	}
	return plukclient.NewClient(config.Config.PlukURL, opts)
}

func initLogging() {
	nopLog := func(string, ...interface{}) {}
	_, _ = maxprocs.Set(maxprocs.Logger(nopLog)) // Ignore error

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
	p.StringVarP(&configPath, "config", "", "", fmt.Sprintf("Path to config file. (default %v)", defaultConfigPath))
	p.StringVarP(&token, "token", "t", "", "Kibernetika AI user token")
	p.StringVarP(&workspace, "workspace", "", "", "Kibernetika AI workspace name (auth method)")
	p.StringVarP(&workspaceSecret, "secret", "", "", "Kibernetika AI workspace secret (auth method)")
	p.StringVar(&oldPlukURL, "url", "", "Base url to dataset service (pluke). Deprecated. Use --pluk-url instead")
	p.StringVar(&baseURL, "base-url", "", "Base url to Kibernetika API.")
	p.StringVar(&plukURL, "pluk-url", "", "Base url to dataset service.")
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
