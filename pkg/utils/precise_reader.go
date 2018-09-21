package utils

import "io"

type PreciseReader struct {
	rd io.Reader
}

func NewPreciseReader(r io.Reader) *PreciseReader {
	return &PreciseReader{rd: r}
}

func (r *PreciseReader) Read(p []byte) (n int, err error) {
	// Read len(p) bytes exactly or throw io.EOF otherwise
	pos := 0
	remain := len(p)
	var read = 0
	for {
		read, err = r.rd.Read(p[pos:])
		if err != nil && err != io.EOF {
			return 0, err
		}

		pos += read
		remain -= read

		if remain == 0 {
			break
		}

		if err == io.EOF {
			return pos, err
		}
	}

	return pos, nil
}
