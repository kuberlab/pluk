package utils

import (
	"crypto/sha512"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	debug              = "DEBUG"
	authValidationVar  = "AUTH_VALIDATION"
	dataVar            = "DATA_DIR"
	gitVar             = "GIT_DIR"
	gitLocalVar        = "GIT_LOCAL_DIR"
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
