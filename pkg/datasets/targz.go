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

	for _, f := range fs.FS {
		fi := f.Fstat
		name := f.Name
		name = strings.TrimPrefix(name, "/")
		if strings.HasPrefix(name, ".") {
			continue
		}
		if fi.IsDir() {
			continue
		}

		logrus.Debugf("Processing file %v", name)

		size := f.Size
		h := &tar.Header{
			Name:    name,
			Mode:    0666,
			Size:    size,
			ModTime: fi.ModTime(),
		}
		if err := twriter.WriteHeader(h); err != nil {
			return err
		}

		_, err := io.Copy(twriter, f)
		if err != nil {
			return err
		}
		resp.Flush()
		//for {
		//	buf := make([]byte, 65536*2)
		//	n, err := f.Read(buf)
		//	if err == io.EOF {
		//		break
		//	}
		//	if _, err := twriter.Write(buf[:n]); err != nil {
		//		return err
		//	}
		//	resp.Flush()
		//}
		f.Close()
	}

	return nil
}
