package logging

import (
	"github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	LogLevel string
)

func InitLogging(cmd *cobra.Command, args []string) {
	logrus.SetFormatter(&logrus.TextFormatter{TimestampFormat: "2006-01-02 15:04:05", FullTimestamp: true})

	lvl, err := logrus.ParseLevel(LogLevel)
	if err != nil {
		logrus.SetLevel(logrus.DebugLevel)
	} else {
		logrus.SetLevel(lvl)
	}
	return
}
