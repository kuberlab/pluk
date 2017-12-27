package datasets

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/pacak/pkg/pacakimpl"
	plukio "github.com/kuberlab/pluk/pkg/io"
)

func WriteTarGz(repo pacakimpl.PacakRepo, ref string, resp *restful.Response) error {
	// Wrap in gzip writer
	zipper := gzip.NewWriter(resp)

	// Wrap in tar writer
	twriter := tar.NewWriter(zipper)
	defer func() {
		twriter.Close()
		zipper.Close()
	}()

	fileInfos, err := repo.ListFilesAtRev(ref)
	if err != nil {
		return err
	}
	for _, fi := range fileInfos {
		if strings.HasPrefix(fi.Name(), ".") {
			continue
		}
		if fi.IsDir() {
			continue
		}

		logrus.Debugf("Processing file %v", fi.Name())

		chunked, err := plukio.NewChunkedFileFromRepo(repo, ref, fi.Name())
		if err != nil {
			return err
		}
		size, err := chunked.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		chunked.Seek(0, io.SeekStart)
		h := &tar.Header{
			Name:    fi.Name(),
			Mode:    0666,
			Size:    size,
			ModTime: fi.ModTime(),
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
	}

	return nil
}
