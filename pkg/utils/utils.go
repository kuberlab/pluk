package utils

import (
	"crypto/sha512"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ApiVersion         = "v1"
	ApiPrefix          = "/pluk/" + ApiVersion
	InternalPrefix     = "/internal"
	debug              = "DEBUG"
	authValidationVar  = "AUTH_VALIDATION"
	internalKeyVar     = "INTERNAL_KEY"
	readConcurrencyVar = "READ_CONCURRENCY"
	dataVar            = "DATA_DIR"
	gitVar             = "GIT_BARE_DIR"
	gitLocalVar        = "GIT_LOCAL_DIR"
	mastersVar         = "MASTERS"
	defaultGitDir      = "/git"
	defaultGitLocalDir = "/git-local"
	defaultDataDir     = "/data"
	ChunkDirLength     = 8
)

func MustParse(date string) time.Time {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", date, time.FixedZone("UTC", 0))
	if err != nil {
		panic(err)
	}
	return t
}

func Bool(b bool) *bool {
	return &b
}

func DebugEnabled() bool {
	debug := os.Getenv(debug)
	if strings.ToLower(debug) == "true" {
		return true
	}
	return false
}

func GitDir() string {
	gitDir := os.Getenv(gitVar)
	if gitDir == "" {
		return defaultGitDir
	}
	return gitDir
}

func GitLocalDir() string {
	gitLocalDir := os.Getenv(gitLocalVar)
	if gitLocalDir == "" {
		return defaultGitLocalDir
	}
	return gitLocalDir
}

func DataDir() string {
	dataDir := os.Getenv(dataVar)
	if dataDir == "" {
		return defaultDataDir
	}
	return dataDir
}

func AuthValidationURL() string {
	return os.Getenv(authValidationVar)
}

func InternalKey() string {
	return os.Getenv(internalKeyVar)
}

func ReadConcurrency() int64 {
	raw := os.Getenv(readConcurrencyVar)
	c, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 4
	}
	return c
}

func Masters() []string {
	mastersRaw := os.Getenv(mastersVar)
	if mastersRaw == "" {
		return make([]string, 0)
	}
	return strings.Split(mastersRaw, ",")
}

func HasMasters() bool {
	return len(Masters()) > 0
}

func String(s string) *string {
	return &s
}

func CalcHash(data []byte) string {
	sum := sha512.Sum512(data)
	return fmt.Sprintf("%x", sum[:])
}

func GetHashedFilename(hash string) string {
	hashDir := hash[:ChunkDirLength]
	hashFile := hash[ChunkDirLength:]
	return fmt.Sprintf("%v/%v/%v", DataDir(), hashDir, hashFile)
}

func PrintEnvInfo() {
	fmt.Printf("DEBUG = %v\n", DebugEnabled())
	fmt.Printf("GIT_BARE_DIR = %q\n", GitDir())
	fmt.Printf("GIT_LOCAL_DIR = %q\n", GitLocalDir())
	fmt.Printf("DATA_DIR = %q\n", DataDir())
	fmt.Printf("AUTH_VALIDATION = %q\n", AuthValidationURL())
	fmt.Printf("MASTERS = %q\n", Masters())
	fmt.Printf("READ_CONCURRENCY = %v\n", ReadConcurrency())
}

func GetFirstN(s []string, n int) []string {
	if n > len(s) {
		n = len(s) - 1
	}
	return s[:n]
}
