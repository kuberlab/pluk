package utils

import (
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Masterminds/semver"
	"github.com/gorilla/websocket"
	"github.com/kuberlab/lib/pkg/types"
)

const (
	ApiVersion           = "v1"
	ApiPrefix            = "/pluk/" + ApiVersion
	InternalPrefix       = "/internal"
	debug                = "DEBUG"
	logLevel             = "LOG_LEVEL"
	authValidationVar    = "AUTH_VALIDATION"
	DoNotSaveChunks      = "DO_NOT_SAVE_CHUNKS"
	internalKeyVar       = "INTERNAL_KEY"
	readConcurrencyVar   = "READ_CONCURRENCY"
	uploadConcurrencyVar = "UPLOAD_CONCURRENCY"
	dataVar              = "DATA_DIR"
	dbPathVar            = "DB_PATH"
	MastersVar           = "MASTERS"
	portVar              = "PLUK_HTTP_PORT"
	defaultPort          = "8082"
	defaultDataDir       = "/data"
	defaultDBPath        = "/pluk/pluke.db"
	ChunkDirLength       = 8
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

func LogLevel() string {
	return os.Getenv(logLevel)
}

func DataDir() string {
	dataDir := os.Getenv(dataVar)
	if dataDir == "" {
		return defaultDataDir
	}
	return dataDir
}

func HttpPort() string {
	port := os.Getenv(portVar)
	if port == "" {
		return defaultPort
	}
	return port
}

func DBPath() string {
	dbPath := os.Getenv(dbPathVar)
	if dbPath == "" {
		return defaultDBPath
	}
	return dbPath
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

func UploadConcurrency() int64 {
	raw := os.Getenv(uploadConcurrencyVar)
	c, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 4
	}
	return c
}

func Masters() []string {
	mastersRaw := os.Getenv(MastersVar)
	if mastersRaw == "" {
		return make([]string, 0)
	}
	return strings.Split(mastersRaw, ",")
}

func SaveChunks() bool {
	dontSave := os.Getenv(DoNotSaveChunks)
	if strings.ToLower(dontSave) == "true" {
		return false
	}
	return true
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

func GetHashFromPath(path string) string {
	hash := strings.TrimPrefix(path, DataDir())
	hash = strings.Replace(hash, "/", "", -1)
	return hash
}

func PrintEnvInfo() {
	fmt.Printf("DEBUG = %v\n", DebugEnabled())
	fmt.Printf("DATA_DIR = %q\n", DataDir())
	fmt.Printf("HTTP_PORT = %q\n", HttpPort())
	fmt.Printf("AUTH_VALIDATION = %q\n", AuthValidationURL())
	fmt.Printf("MASTERS = %q\n", Masters())
	fmt.Printf("READ_CONCURRENCY = %v\n", ReadConcurrency())
	fmt.Printf("SAVE_CHUNKS = %v\n", SaveChunks())
}

func GetFirstN(s []string, n int) []string {
	if n > len(s) {
		n = len(s)
	}
	return s[:n]
}

// exists returns whether the given file or directory exists or not
func Exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func WriteMessage(ws *websocket.Conn, sType, id string, content interface{}) error {
	msg := types.Message{
		Type:    sType,
		ID:      id,
		Content: content,
	}
	return ws.WriteJSON(msg)
}

func Assert(want, got interface{}, t *testing.T) {
	if want == nil && got == nil {
		return
	}
	if !reflect.DeepEqual(want, got) {
		_, file, line, _ := runtime.Caller(1)
		splitted := strings.Split(file, string(os.PathSeparator))
		t.Fatalf("%v:%v: Failed: got %v, want %v", splitted[len(splitted)-1], line, got, want)
	}
}

func LoadAsJson(m map[string]interface{}, v interface{}) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func CheckVersion(version string) error {
	v, err := semver.NewVersion(version)
	if err != nil {
		reg := "version examples: 1.0.1, 1.5.0-dev, 1.8.1-alpha.1"
		return fmt.Errorf("%v: %v; %v", version, err.Error(), reg)
	}
	if v.String() != version {
		return fmt.Errorf("Version must be a valid semantic version. Given %v, try to save as version %v", version, v.String())
	}
	return nil
}
