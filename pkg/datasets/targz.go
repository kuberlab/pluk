package datasets

import (
	"archive/tar"
	"fmt"
	"io"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	plukio "github.com/kuberlab/pluk/pkg/io"
)

func WriteTar(fs *plukio.ChunkedFileFS, resp *restful.Response) error {
	// Wrap in tar writer
	twriter := tar.NewWriter(resp)
	defer func() {
		twriter.Close()
	}()

	prevName := ""
	err := fs.Walk("/", func(path string, f *plukio.ChunkedFile, err error) error {
		name := strings.TrimPrefix(path, "/")
		if strings.HasPrefix(name, ".") || path == "/" {
			return nil
		}
		if f.Fstat.IsDir() {
			//h := &tar.Header{
			//	Name:     name,
			//	Mode:     0644,
			//	Typeflag: tar.TypeDir,
			//	ModTime:  f.Fstat.ModTime(),
			//}
			//if err := twriter.WriteHeader(h); err != nil {
			//	return fmt.Errorf("Failed write file %v: %v", prevName, err)
			//}
			//hd++
			//sz += 512
			return nil
		}
		logrus.Debugf("Processing file %v, size=%v", name, f.Size)

		size := f.Size

		h := &tar.Header{
			Name:    name,
			Mode:    int64(f.Fstat.Mode()),
			Size:    size,
			ModTime: f.Fstat.ModTime(),
		}
		if err := twriter.WriteHeader(h); err != nil {
			return fmt.Errorf("Failed write file %v: %v", prevName, err)
		}
		_, err = io.Copy(twriter, f)
		if err != nil {
			return fmt.Errorf("Failed write file %v: %v", name, err)
		}
		prevName = name
		resp.Flush()
		f.Close()
		return nil
	})
	return err
}
