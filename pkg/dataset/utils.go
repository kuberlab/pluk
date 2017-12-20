package dataset

import (
	"time"

	"strings"

	"github.com/gogits/git-module"
	"github.com/kuberlab/pacak/pkg/pacakimpl"
)

func initRepo(git pacakimpl.GitInterface, repo string, create bool) (pacakimpl.PacakRepo, error) {
	pacakRepo, err := git.GetRepository(repo)
	if err != nil {
		if !create {
			return nil, err
		}
		if err = git.InitRepository(getCommitter(), repo, []pacakimpl.GitFile{}); err != nil {
			return nil, err
		}
	}

	if pacakRepo == nil {
		pacakRepo, err = git.GetRepository(repo)
		if err != nil {
			return nil, err
		}
	}
	return pacakRepo, nil
}

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
