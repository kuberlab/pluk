package dataset

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gogits/git-module"
	"github.com/kuberlab/pacak/pkg/pacakimpl"
	"strings"
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

func iterateOverTarGz(targz io.ReadCloser, action func(name string, data []byte) error) error {
	gzf, err := gzip.NewReader(targz)
	if err != nil {
		return err
	}

	tarReader := tar.NewReader(gzf)

	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		name := header.Name

		switch header.Typeflag {
		case tar.TypeDir:
			continue
		case tar.TypeReg:
			logrus.Debugf("processing %v..", name)
			data := make([]byte, header.Size)
			_, err = tarReader.Read(data)
			if err != nil {
				return err
			}
			if err = action(name, data); err != nil {
				return err
			}

		default:
			return fmt.Errorf("Unable to figure out type %v in file %v.", header.Typeflag, name)
		}
	}
	return nil
}
