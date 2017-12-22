package dataset

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	plukio "github.com/kuberlab/pluk/pkg/io"
)

func WriteTarGz(root string, resp *restful.Response) error {
	// Wrap in gzip writer
	zipper := gzip.NewWriter(resp)

	// Wrap in tar writer
	twriter := tar.NewWriter(zipper)
	defer func() {
		twriter.Close()
		zipper.Close()
	}()

	now := time.Now()

	err := filepath.Walk(root, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		sinceRoot := strings.TrimPrefix(path, root+"/")
		if strings.HasPrefix(sinceRoot, ".") {
			return nil
		}
		if f.IsDir() {
			return nil
		}

		logrus.Debugf("Processing file %v", path)

		size, reader, errF := plukio.GetRealFileReader(path)
		if errF != nil {
			return errF
		}
		h := &tar.Header{
			Name:    sinceRoot,
			Mode:    0666,
			Size:    int64(size),
			ModTime: now,
		}
		if err := twriter.WriteHeader(h); err != nil {
			return err
		}

		for {
			content, err := plukio.GetNextRealChunk(reader)
			if err == io.EOF {
				break
			}
			if _, err := twriter.Write(content); err != nil {
				return err
			}
			resp.Flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
