package main

import (
	"os"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/kuberlab/pluk/pkg/fuse"
	"github.com/kuberlab/pluk/pkg/utils"
	"github.com/spf13/cobra"
)

const (
	defaultLogLevel = "info"
)

var (
	debug    bool
	logLevel string
)

func initLogging() {
	logrus.SetFormatter(&logrus.TextFormatter{TimestampFormat: "2006-01-02 15:04:05", FullTimestamp: true})

	if debug {
		logLevel = "debug"
		os.Setenv("DEBUG", "true")
	}
	lvl, err := logrus.ParseLevel(logLevel)
	if err != nil {
		logrus.SetLevel(logrus.DebugLevel)
	} else {
		logrus.SetLevel(lvl)
	}
	return
}

func initRoot(cmd *cobra.Command, args []string) error {
	initLogging()
	return nil
}

type plukeFSCmd struct {
	mountPoint string
	workspace  string
	dataset    string
	version    string
	server     string
	secret     string
}

func newPlukeFSCmd() *cobra.Command {
	plukeFS := &plukeFSCmd{}
	opts := make([]string, 0)
	var cmd = &cobra.Command{
		Use:               "plukefs",
		Short:             "Fuse-mount for pluke.",
		PersistentPreRunE: initRoot,
		//Version:           GetVersion().String(),
		Run: func(cmd *cobra.Command, args []string) {
			for _, opt := range opts {
				splitted := strings.Split(opt, "=")
				if len(splitted) != 2 {
					logrus.Errorf("Wrong option: %v; must be in form name=value.", opt)
					return
				}
				name := splitted[0]
				value := splitted[1]
				switch name {
				case "workspace":
					plukeFS.workspace = value
				case "dataset":
					plukeFS.dataset = value
				case "version":
					plukeFS.version = value
				case "server":
					plukeFS.server = value
				case "secret":
					plukeFS.secret = value
				case "mountPoint":
					plukeFS.mountPoint = value
				default:
					logrus.Errorf("Unrecognized option: %v", name)
					return
				}
			}
			plukeFS.run()
		},
	}

	p := cmd.PersistentFlags()
	// Declare common arguments.
	p.StringVar(&logLevel, "log-level", defaultLogLevel, "Logging level. One of (debug, info, warning, error)")
	p.BoolVarP(&debug, "debug", "", false, "Enable debug level (shortcut for --log-level=debug).")
	p.StringSliceVarP(&opts, "option", "o", opts, "Options for mount. Pass them as -o name=value")

	// Add all commands
	return cmd
}

func (cmd *plukeFSCmd) run() {
	if cmd.workspace == "" {
		logrus.Error("workspace is undefined.")
		return
	}
	if cmd.dataset == "" {
		logrus.Error("dataset is undefined.")
		return
	}
	if cmd.version == "" {
		logrus.Error("version is undefined.")
		return
	}
	if cmd.server == "" {
		logrus.Error("server is undefined.")
		return
	}
	if cmd.mountPoint == "" {
		logrus.Error("mountPoint is undefined.")
		return
	}

	logrus.Debugf("Start with workspace=%v", cmd.workspace)
	logrus.Debugf("Start with dataset=%v", cmd.dataset)
	logrus.Debugf("Start with version=%v", cmd.version)
	logrus.Debugf("Start with server=%v", cmd.server)
	logrus.Debugf("Start with secret=%v", cmd.secret)

	os.Setenv(utils.DoNotSaveChunks, "true")
	os.Setenv(utils.MastersVar, cmd.server)

	utils.PrintEnvInfo()

	plukfs, err := fuse.NewPlukFS(
		cmd.workspace,
		cmd.dataset,
		cmd.version,
		cmd.server,
		cmd.secret,
	)
	if err != nil {
		logrus.Error(err)
		return
	}

	fs := pathfs.NewPathNodeFs(pathfs.NewReadonlyFileSystem(plukfs), &pathfs.PathNodeFsOptions{Debug: debug})
	server, _, err := nodefs.MountRoot(cmd.mountPoint, fs.Root(), &nodefs.Options{Debug: debug})
	if err != nil {
		logrus.Error(err)
		return
	}
	server.Serve()
}

func main() {
	cmd := newPlukeFSCmd()
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
