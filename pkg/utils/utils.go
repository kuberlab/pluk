package utils

import (
	"crypto/sha512"
	"errors"
	"fmt"
	"github.com/Sirupsen/logrus"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Masterminds/semver"
	"github.com/gorilla/websocket"
	"github.com/json-iterator/go"
	"github.com/kuberlab/lib/pkg/types"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

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
	dbNameVar            = "DB_NAME"
	dbHostVar            = "DB_HOST"
	dbUserVar            = "DB_USER"
	dbPassVar            = "DB_PASSWORD"
	dbPortVar            = "DB_PORT"
	MastersVar           = "MASTERS"
	portVar              = "PLUK_HTTP_PORT"
	PortGrpcVar          = "PLUK_GRPC_PORT"
	prettyPrintVar       = "PRETTY_PRINT"
	defaultPort          = "8082"
	defaultGrpcPort      = "8085"
	defaultDataDir       = "/data"
	defaultDBName        = "/pluk/pluke.db"
	ChunkDirLength       = 8
)

var (
	DataDirValue = ""
	AuthURL      = "unset"
	UseGrpc      = false
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

func PrettyPrintEnabled() bool {
	pp := os.Getenv(prettyPrintVar)
	return strings.ToLower(pp) == "true"
}

func LogLevel() string {
	return os.Getenv(logLevel)
}

func DataDir() string {
	if DataDirValue != "" {
		return DataDirValue
	}

	dataDir := os.Getenv(dataVar)
	if dataDir == "" {
		DataDirValue = defaultDataDir
		return defaultDataDir
	}
	DataDirValue = dataDir
	return dataDir
}

func HttpPort() string {
	port := os.Getenv(portVar)
	if port == "" {
		return defaultPort
	}
	return port
}

func GrpcPort() string {
	port := os.Getenv(PortGrpcVar)
	if port == "" {
		return defaultGrpcPort
	}
	return port
}

//func UseGrpc() bool {
//	debug := os.Getenv(debug)
//	if strings.ToLower(debug) == "true" {
//		return true
//	}
//	return false
//}

func DBName() string {
	return FromEnv(dbNameVar, defaultDBName)
}

func DBHost() string {
	return FromEnv(dbHostVar, "")
}

func DBUser() string {
	return FromEnv(dbUserVar, "")
}

func DBPassword() string {
	return FromEnv(dbPassVar, "")
}

func DBPort() string {
	return FromEnv(dbPortVar, "")
}

func FromEnv(varName, defaultVal string) string {
	val := os.Getenv(varName)
	if val == "" {
		val = defaultVal
	}
	return val
}

func AuthValidationURL() string {
	if AuthURL != "unset" {
		return AuthURL
	}
	AuthURL = os.Getenv(authValidationVar)
	return AuthURL
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

func DBType() string {
	dbType := os.Getenv("DB_TYPE")
	if dbType == "" {
		dbType = "sqlite3"
	}
	return dbType
}

func UploadConcurrency() int64 {
	raw := os.Getenv(uploadConcurrencyVar)
	c, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		if DBType() == "sqlite3" {
			return 1
		}
		return 8
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
	//sum := sha256.Sum256(data)
	sum := sha512.Sum512(data)
	return fmt.Sprintf("%x", sum[:])
}

func GetHashedFilename(hash string, version byte) string {
	if version == 2 {
		return fmt.Sprintf("%v/%v/%v/%v", DataDir(), hash[:2], hash[2:4], hash[4:])
	} else if version == 1 {
		return fmt.Sprintf("%v/%v/%v/%v/%v", DataDir(), hash[:2], hash[2:4], hash[4:6], hash[6:])
	} else if version == 0 {
		hashDir := hash[:ChunkDirLength]
		hashFile := hash[ChunkDirLength:]
		return fmt.Sprintf("%v/%v/%v", DataDir(), hashDir, hashFile)
	} else {
		return ""
	}
}

func GetHashFromPath(path string) (hash string, version byte) {
	hash = strings.TrimPrefix(path, DataDir())
	cnt := strings.Count(hash, "/")
	if cnt == 3 {
		version = 2
	} else if cnt == 4 {
		version = 1
	} else {
		version = 0
	}
	hash = strings.Replace(hash, "/", "", -1)
	return
}

func PrintEnvInfo() {
	fmt.Printf("DEBUG = %v\n", DebugEnabled())
	fmt.Printf("DATA_DIR = %q\n", DataDir())
	fmt.Printf("HTTP_PORT = %q\n", HttpPort())
	fmt.Printf("AUTH_VALIDATION = %q\n", AuthValidationURL())
	fmt.Printf("MASTERS = %q\n", Masters())
	fmt.Printf("READ_CONCURRENCY = %v\n", ReadConcurrency())
	fmt.Printf("UPLOAD_CONCURRENCY = %v\n", UploadConcurrency())
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

func Retry(description string, delaySec float64, retries int, f interface{}, arg ...interface{}) (res interface{}, err error) {
	vf := reflect.ValueOf(f)
	valuesArgs := make([]reflect.Value, 0)

	if vf.Kind() != reflect.Func {
		err = errors.New(fmt.Sprintf("%v is not a Func!", vf.String()))
		return
	}

	for _, v := range arg {
		valuesArgs = append(valuesArgs, reflect.ValueOf(v))
	}

	run := func() (interface{}, error) {
		res := vf.Call(valuesArgs)
		last := res[len(res)-1]
		var first interface{} = nil
		if len(res) >= 2 {
			raw := res[0]
			if !raw.IsNil() {
				first = raw.Interface()
			}
		}

		if last.IsNil() {
			return first, nil
		}
		errF := last.Interface().(error)

		return first, errF
	}
	res, err = run()

	if err == nil {
		return res, nil
	}

	//timeoutDur := time.Duration(int64(float64(time.Second) * timeoutSec))
	delayDur := time.Duration(int64(float64(time.Second) * delaySec))
	//timeout := time.NewTimer(timeoutDur)
	sleep := time.NewTicker(delayDur)

	//defer timeout.Stop()
	defer sleep.Stop()

	step := 1
	for {
		select {
		case <-sleep.C:
			logrus.Warningf("Retry(%v) call: %v", step, description)

			res, err = run()

			if err == nil {
				return res, nil
			}
			step++
			if step + 1 >= retries {
				return res, errors.New(
					fmt.Sprintf(
						"Max retries (%v) exceeded while waiting for %v: %v",
						retries, vf.String(), err,
					),
				)
			}
		//case <-timeout.C:
		//	return res, errors.New(fmt.Sprintf("Timeout while waiting for %v: %v", vf.String(), err))
		}
	}
}

type FakeWriter struct {
	Written int
}

func (w *FakeWriter) Write(b []byte) (int, error) {
	w.Written += len(b)
	return len(b), nil
}
