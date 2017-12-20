package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/lib/pkg/types"
)

type LogRecordHandler struct {
	http.ResponseWriter
	status int
}

func (r *LogRecordHandler) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *LogRecordHandler) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
func (r *LogRecordHandler) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := r.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, fmt.Errorf("not a Hijacker")
}

func WrapLogger(f http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record := &LogRecordHandler{
			ResponseWriter: w,
			status:         200,
		}
		t := time.Now()
		f.ServeHTTP(record, r)
		logrus.Infof("%v %v => %v, %v", r.Method, r.RequestURI, record.status, time.Since(t))
	})
}

type ResponseError struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
}

func WriteErrorString(resp *restful.Response, status int, message string) {
	logrus.Errorf("Request error: %d - %s", status, message)
	err := errors.NewStatus(status, message)
	resp.WriteHeaderAndEntity(status, err)
}

func WriteStatusError(resp *restful.Response, status int, err error) {
	if mlerr, ok := err.(*errors.Error); ok {
		if mlerr.Status != status {
			status = mlerr.Status
		}
		logrus.Errorf("Request error: %d - %v;%v", status, mlerr.Message, mlerr.Reason)
		resp.WriteHeaderAndEntity(status, mlerr)
	} else {
		WriteErrorString(resp, status, err.Error())
	}
}

func WriteError(resp *restful.Response, err error) {
	if mlerr, ok := err.(*errors.Error); ok {
		logrus.Errorf("Request error: %d - %v: %v", mlerr.Status, mlerr.Message, mlerr.Reason)
		resp.WriteHeaderAndEntity(mlerr.Status, mlerr)
	} else {
		WriteErrorString(resp, http.StatusInternalServerError, err.Error())
	}
}

func NotFoundHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)

		resp := errors.NewStatus(
			http.StatusNotFound,
			fmt.Sprintf("URI '%v' not found", r.RequestURI),
		)
		data, _ := json.MarshalIndent(resp, "", "  ")
		w.Write(data)
		w.Write([]byte("\n"))
	})
}

func getBoolQueryParam(req *restful.Request, param string) bool {
	v := req.QueryParameter(param)
	bVal, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return bVal
}

func GetQueryParamAs(req *restful.Request, name string, typeExample interface{}, optional bool) interface{} {
	param := req.QueryParameter(name)
	var err error

	defer func() {
		if err != nil {
			logrus.Errorf("Invalid %v: %v", name, err)
		}
	}()

	if param == "" {
		if !optional {
			err = fmt.Errorf("required '%v'", name)
		}
		return nil
	}

	switch typeExample.(type) {
	case int:
		var value int64
		value, err = strconv.ParseInt(param, 10, 32)
		val := new(int)
		*val = int(value)
		return val
	case uint:
		var value uint64
		value, err = strconv.ParseUint(param, 10, 32)
		val := new(uint)
		*val = uint(value)
		return val
	case bool:
		var value bool
		value, err = strconv.ParseBool(param)
		return &value
	case string:
		return &param
	case time.Time:
		var value time.Time
		value, err = time.ParseInLocation(types.Format, param, time.FixedZone("UTC", 0))
		return &value
	}
	return &param
}

// GetQueryParamInt gets query int parameter
func GetQueryParamInt(req *restful.Request, name string, optional bool) *int {
	val := GetQueryParamAs(req, name, int(1), optional)
	if val == nil {
		return nil
	}
	return val.(*int)
}

// GetQueryParamUint gets query uint parameter
func GetQueryParamUint(req *restful.Request, name string, optional bool) *uint {
	val := GetQueryParamAs(req, name, uint(1), optional)
	if val == nil {
		return nil
	}
	return val.(*uint)
}

// GetQueryParamBool gets query bool parameter
func GetQueryParamBool(req *restful.Request, name string, optional bool) *bool {
	val := GetQueryParamAs(req, name, bool(false), optional)
	if val == nil {
		return nil
	}
	return val.(*bool)
}

// GetQueryParamDateTime gets query of date time parameter
func GetQueryParamDateTime(req *restful.Request, name string, optional bool) *time.Time {
	val := GetQueryParamAs(req, name, time.Time{}, optional)
	if val == nil {
		return nil
	}
	return val.(*time.Time)
}
