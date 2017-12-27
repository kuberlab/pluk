package datasets

import (
	"time"

	"strings"

	"github.com/gogits/git-module"
)

func getCommitter() git.Signature {
	return git.Signature{
		Name:  Author,
		Email: AuthorEmail,
		When:  time.Now(),
	}
}

type CommitMessage struct {
	Comment string
	Version string
}

func parseMessage(message string) *CommitMessage {
	lines := strings.Split(message, "\n")
	for _, line := range lines {
		splitted := strings.Split(line, ": ")
		if len(splitted) != 2 {
			return nil
		}
		if splitted[0] == "Version" {
			return &CommitMessage{Version: splitted[1]}
		}
	}
	return nil
}
