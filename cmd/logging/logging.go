package logging

import (
	"github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/kuberlab/pluk/cmd/client"
)


func InitLogging(cmd *cobra.Command, args []string) {
	logrus.SetFormatter(&logrus.TextFormatter{TimestampFormat: "2006-01-02 15:04:05", FullTimestamp: true})

	lvl, err := logrus.ParseLevel(client.Settings.LogLevel)
	if err != nil {
		logrus.SetLevel(logrus.DebugLevel)
	} else {
		logrus.SetLevel(lvl)
	}
	return
}
