package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/kuberlab/pluk/pkg/utils"
)

type LogRecordHandler struct {
	http.ResponseWriter
	status int
}

func (r *LogRecordHandler) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
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
			status:         0,
		}
		t := time.Now()
		f.ServeHTTP(record, r)

		if record.status == 0 {
			record.status = http.StatusOK
		}

		logrus.Infof("%v %v => %v, %v", r.Method, r.RequestURI, record.status, time.Since(t))
	})
}

type ResponseError struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
}

func EntityNotFoundError(req *restful.Request, name string) error {
	return errors.NewStatus(http.StatusNotFound, fmt.Sprintf("%v '%v' not found", strings.Title(currentType(req)), name))
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

func (api *API) InternalHook(req *restful.Request, resp *restful.Response, filter *restful.FilterChain) {
	masterClient := plukclient.NewInternalMasterClient()
	req.SetAttribute("masterclient", masterClient)

	filter.ProcessFilter(req, resp)
}

func (api *API) AuthHook(req *restful.Request, resp *restful.Response, filter *restful.FilterChain) {
	internal := req.HeaderParameter("Internal")
	if internal != "" && utils.InternalKey() == internal {
		filter.ProcessFilter(req, resp)
		return
	}

	authURL := utils.AuthValidationURL()
	if authURL == "" && !utils.HasMasters() {
		filter.ProcessFilter(req, resp)
		return
	}

	authHeader := req.HeaderParameter("Authorization")
	cookie := req.HeaderParameter("Cookie")
	secret := req.HeaderParameter("X-Workspace-Secret")
	ws := req.HeaderParameter("X-Workspace-Name")
	key := authHeader + cookie + ws + secret

	masterClient := plukclient.NewMasterClientFromHeaders(req.Request.Header)
	req.SetAttribute("masterclient", masterClient)

	if api.cache.Get(key) {
		filter.ProcessFilter(req, resp)
		return
	} else {
		if utils.HasMasters() {
			// Talk to master.
			logrus.Debugf("Auth request to master %v", utils.Masters()[0])
			_, err := masterClient.ListEntities(currentType(req), "kuberlab")
			if err != nil {
				WriteErrorString(resp, http.StatusUnauthorized, err.Error())
				return
			}
		} else if ws != "" && secret != "" {
			u, err := url.Parse(authURL)
			if err != nil {
				WriteError(resp, err)
				return
			}
			validationURL := fmt.Sprintf("%v://%v/api/v0.2/secret/%v", u.Scheme, u.Host, secret)
			request, _ := http.NewRequest("GET", validationURL, nil)
			logrus.Debugf("GET %v://%v/[redacted]", request.URL.Scheme, request.URL.Host)
			r, err := api.client.Do(request)
			if err != nil {
				http.Error(resp, err.Error(), http.StatusInternalServerError)
				return
			}
			logrus.Debugf("Got %v", r.StatusCode)
			if r.StatusCode >= 400 {
				logrus.Error(fmt.Sprintf("Invalid auth to %v://%v/[redacted]", request.URL.Scheme, request.URL.Host))
				WriteErrorString(resp, http.StatusUnauthorized, "Unauthorized.")
				return
			}
		} else {
			request, _ := http.NewRequest("GET", authURL, nil)
			request.Header.Add("Cookie", cookie)
			request.Header.Add("Authorization", authHeader)

			logrus.Debugf("GET %v", request.URL)

			r, err := api.client.Do(request)
			if err != nil {
				WriteStatusError(resp, http.StatusInternalServerError, err)
				return
			}
			logrus.Debugf("Got %v", r.StatusCode)
			if r.StatusCode >= 400 {
				WriteStatusError(resp, r.StatusCode, fmt.Errorf("Cannot authenticate to %v", authURL))
				return
			}
		}

		api.cache.Set(key, true)
	}

	filter.ProcessFilter(req, resp)
}

func setCurrentType(req *restful.Request, resp *restful.Response, filter *restful.FilterChain) {
	eType, ok := req.PathParameters()["entityType"]
	if !ok {
		// Skip
		filter.ProcessFilter(req, resp)
		return
	}

	_, ok = plukclient.AllowedTypes[eType]
	if ok {
		req.SetAttribute("entityType", eType)
		filter.ProcessFilter(req, resp)
		return
	} else {
		NotFoundHandler().ServeHTTP(resp.ResponseWriter, req.Request)
	}
}

func currentType(req *restful.Request) string {
	sTypeRaw := req.Attribute("entityType")
	sType, ok := sTypeRaw.(string)
	if !ok {
		sType = "dataset"
	}
	return sType
}
