package datasets

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	plukio "github.com/kuberlab/pluk/pkg/io"
)

func WriteTarGz(fs *plukio.ChunkedFileFS, resp *restful.Response) error {
	// Wrap in gzip writer
	zipper := gzip.NewWriter(resp)

	// Wrap in tar writer
	twriter := tar.NewWriter(zipper)
	defer func() {
		twriter.Close()
		zipper.Close()
	}()

	//fileInfos, err := repo.ListFilesAtRev(ref)
	//if err != nil {
	//	return err
	//}
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

		//chunked, err := plukio.NewChunkedFileFromRepo(repo, ref, name)
		//if err != nil {
		//	return err
		//}
		size := f.Size
		//if err != nil {
		//	return err
		//}
		//f.Seek(0, io.SeekStart)
		h := &tar.Header{
			Name:    name,
			Mode:    0666,
			Size:    size,
			ModTime: fi.ModTime(),
		}
		if err := twriter.WriteHeader(h); err != nil {
			return err
		}

		for {
			buf := make([]byte, 1048576)
			n, err := f.Read(buf)
			if err == io.EOF {
				break
			}
			if _, err := twriter.Write(buf[:n]); err != nil {
				return err
			}
			resp.Flush()
		}
		f.Close()
	}

	return nil
}
