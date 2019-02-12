package datasets

import (
	"archive/tar"
	"fmt"
	"io"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	plukio "github.com/kuberlab/pluk/pkg/io"
)

func WriteTar(fs *plukio.ChunkedFileFS, resp *restful.Response) error {
	// Wrap in tar writer
	twriter := tar.NewWriter(resp.ResponseWriter)
	defer func() {
		twriter.Close()
	}()

	prevName := ""
	err := fs.Walk("/", func(path string, f *plukio.ChunkedFile, err error) error {
		name := path
		// Inline strings.TrimPrefix(): more performance
		if len(path) >= len("/") && path[:1] == "/" {
			name = path[1:]
		}

		// Inlining function: more performance
		//if strings.HasPrefix(name, ".") || path == "/" {
		if (len(name) >= len(".") && name[:1] == ".") || path == "/" {
			return nil
		}
		if f.Dir {
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

		h := &tar.Header{
			Name:    name,
			Mode:    int64(f.Mode),
			Size:    f.Size,
			ModTime: f.ModTime,
		}
		if err := twriter.WriteHeader(h); err != nil {
			return fmt.Errorf("Failed write file %v: %v", prevName, err)
		}
		_, err = io.Copy(twriter, f)
		if err != nil {
			return fmt.Errorf("Failed write file %v: %v", name, err)
		}
		prevName = name
		//resp.Flush()
		f.Close()
		return nil
	})
	return err
}
