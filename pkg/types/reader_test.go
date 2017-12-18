package types

import (
	"testing"
	"crypto/sha512"
	"fmt"
	"strings"
	"io"
)

func TestReader(t *testing.T) {
	test := "1234567890123456789000"
	chunkSize := 10
	sum := sha512.Sum512([]byte(test[0:10]))
	a := fmt.Sprintf("%x", sum[:])
	sum = sha512.Sum512([]byte(test[20:]))
	b := fmt.Sprintf("%x", sum[:])
	res := []string{a,a,b}
	for readSize := 1 ; readSize < 24; readSize++ {
		r := NewChunkedReader(chunkSize, strings.NewReader(test))
		for {
			b := make([]byte, readSize)
			_, err := r.Read(b)
			if err == io.EOF {
				break
			}
		}
		testFunc := func(should []string, actual []string) string {
			if len(should) != len(actual) {
				return fmt.Sprintf("Size %d. Expected %d chunks, got %d.",readSize, len(should), len(actual))
			}
			for i := range should {
				if should[i] != actual[i] {
					return fmt.Sprintf("Size %d. Expected %s for %d'th chunks , got %s.",readSize, should[i], i, actual[i])
				}
			}
			return ""
		}
		if e := testFunc(res, r.Chunks); e != "" {
			t.Error(e)
		}
	}

}

