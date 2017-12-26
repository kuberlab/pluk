package datasets

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

	err := filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		sinceRoot := strings.TrimPrefix(path, root+"/")
		if strings.HasPrefix(sinceRoot, ".") {
			return nil
		}
		if fi.IsDir() {
			return nil
		}

		logrus.Debugf("Processing file %v", path)

		f, errF := os.Open(path)
		if errF != nil {
			return err
		}
		chunked, errF := plukio.NewChunkedFile(f)
		if errF != nil {
			return errF
		}
		chunkedFi, errF := chunked.Stat()
		if errF != nil {
			return errF
		}
		h := &tar.Header{
			Name:    sinceRoot,
			Mode:    0666,
			Size:    chunkedFi.Size(),
			ModTime: now,
		}
		if err := twriter.WriteHeader(h); err != nil {
			return err
		}

		for {
			buf := make([]byte, 1048576)
			n, err := chunked.Read(buf)
			if err == io.EOF {
				break
			}
			if _, err := twriter.Write(buf[:n]); err != nil {
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
