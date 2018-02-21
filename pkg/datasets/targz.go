package datasets

import (
	"archive/tar"
	"io"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	plukio "github.com/kuberlab/pluk/pkg/io"
)

func WriteTarGz(fs *plukio.ChunkedFileFS, resp *restful.Response) error {

	// Wrap in tar writer
	twriter := tar.NewWriter(resp)
	defer func() {
		twriter.Close()
	}()

	err := fs.Walk("/", func(path string, f *plukio.ChunkedFile, err error) error {
		if f.Fstat.IsDir() {
			return nil
		}
		name := strings.TrimPrefix(path, "/")
		if strings.HasPrefix(name, ".") {
			return nil
		}
		logrus.Debugf("Processing file %v", name)

		size := f.Size

		h := &tar.Header{
			Name:    name,
			Mode:    0666,
			Size:    size,
			ModTime: f.Fstat.ModTime(),
		}
		if err := twriter.WriteHeader(h); err != nil {
			return err
		}
		_, err = io.Copy(twriter, f)
		if err != nil {
			return err
		}
		resp.Flush()
		f.Close()
		return nil
	})
	return err
}
