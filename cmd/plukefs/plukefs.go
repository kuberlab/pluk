package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/Sirupsen/logrus"
	gofuse "github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/kuberlab/pluk/pkg/fuse"
	"github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/kuberlab/pluk/pkg/utils"
	"github.com/spf13/cobra"
	"os/signal"
)

const (
	defaultLogLevel = "info"
)

var (
	debug    bool
	debugFS  bool
	logLevel string
)

func initLogging() {
	logrus.SetFormatter(&logrus.TextFormatter{TimestampFormat: "2006-01-02 15:04:05", FullTimestamp: true})

	if debug {
		logLevel = "debug"
		_ = os.Setenv("DEBUG", "true")
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
	mountPoint      string
	objectWorkspace string
	secretWorkspace string
	name            string
	version         string
	server          string
	secret          string
	dsType          string
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
				case "object_workspace":
					plukeFS.objectWorkspace = value
				case "secret_workspace":
					plukeFS.secretWorkspace = value
				case "name":
					plukeFS.name = value
				case "version":
					plukeFS.version = value
				case "server":
					plukeFS.server = value
				case "secret":
					plukeFS.secret = value
				case "mountPoint":
					plukeFS.mountPoint = value
				case "type":
					plukeFS.dsType = value
				default:
					logrus.Errorf("Unrecognized option: %v", name)
					return
				}
			}
			code := plukeFS.run()
			os.Exit(code)
		},
	}

	p := cmd.PersistentFlags()
	// Declare common arguments.
	p.StringVar(&logLevel, "log-level", defaultLogLevel, "Logging level. One of (debug, info, warning, error)")
	p.BoolVarP(&debug, "debug", "", false, "Enable debug level (shortcut for --log-level=debug).")
	p.BoolVarP(&debugFS, "debug-fs", "", false, "Enable debug level (shortcut for --log-level=debug).")
	p.StringSliceVarP(&opts, "option", "o", opts, "Options for mount. Pass them as -o name=value")

	// Add all commands
	return cmd
}

func (cmd *plukeFSCmd) run() int {
	if cmd.objectWorkspace == "" {
		logrus.Error("object_workspace is undefined.")
		return 1
	}
	if cmd.secretWorkspace == "" {
		logrus.Error("secret_workspace is undefined.")
		return 1
	}
	if cmd.name == "" {
		logrus.Error("name is undefined.")
		return 1
	}
	if cmd.version == "" {
		logrus.Error("version is undefined.")
		return 1
	}
	if cmd.server == "" {
		logrus.Error("server is undefined.")
		return 1
	}
	if cmd.mountPoint == "" {
		logrus.Error("mountPoint is undefined.")
		return 1
	}

	logrus.Debugf("Start with object_workspace=%v", cmd.objectWorkspace)
	logrus.Debugf("Start with name=%v", cmd.name)
	logrus.Debugf("Start with version=%v", cmd.version)
	logrus.Debugf("Start with server=%v", cmd.server)
	logrus.Debugf("Start with secret_workspace=%v", cmd.secretWorkspace)
	logrus.Debugf("Start with secret=%v", cmd.secret)

	_ = os.Setenv(utils.DoNotSaveChunks, "true")
	_ = os.Setenv(utils.MastersVar, cmd.server)

	io.MasterClient = plukclient.NewMasterClientWithSecret(cmd.secretWorkspace, cmd.secret)
	if logrus.GetLevel() == logrus.DebugLevel {
		utils.PrintEnvInfo()
	}

	plukfs, err := fuse.NewPlukFS(
		cmd.dsType,
		cmd.objectWorkspace,
		cmd.name,
		cmd.version,
		cmd.server,
		cmd.secret,
		cmd.secretWorkspace,
	)
	if err != nil {
		fmt.Println(err)
		return 1
	}

	fs := pathfs.NewPathNodeFs(pathfs.NewReadonlyFileSystem(plukfs), &pathfs.PathNodeFsOptions{Debug: debugFS})
	server, _, err := MountRoot(cmd.mountPoint, fs.Root(), &nodefs.Options{Debug: debugFS})
	if err != nil {
		fmt.Println(err)
		return 1
	}
	logrus.Info("FS is ready!")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			logrus.Info("Shutdown fs...")
			_ = server.Unmount()
			os.Exit(0)
		}
	}()

	server.Serve()

	return 0
}

// Mounts a filesystem with the given root node on the given directory.
// Convenience wrapper around fuse.NewServer
func MountRoot(mountpoint string, root nodefs.Node, opts *nodefs.Options) (*gofuse.Server, *nodefs.FileSystemConnector, error) {
	conn := nodefs.NewFileSystemConnector(root, opts)

	mountOpts := gofuse.MountOptions{}
	if opts != nil && opts.Debug {
		mountOpts.Debug = opts.Debug
	}
	mountOpts.Name = "plukefs"
	mountOpts.FsName = "plukefs"
	s, err := gofuse.NewServer(conn.RawFS(), mountpoint, &mountOpts)
	if err != nil {
		return nil, nil, err
	}
	return s, conn, nil
}

func main() {
	cmd := newPlukeFSCmd()
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
