package api

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/emicklei/go-restful"
	"github.com/json-iterator/go"
	"github.com/kuberlab/lib/pkg/dealerclient"
	"github.com/kuberlab/lib/pkg/errors"
	"github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"github.com/kuberlab/pluk/pkg/utils"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

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
	cache := utils.NewRequestCache()
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

		authHeader := r.Header.Get("Authorization")
		cookie := r.Header.Get("Cookie")
		secret := r.Header.Get("X-Workspace-Secret")
		ws := r.Header.Get("X-Workspace-Name")
		internal := r.Header.Get("Internal")

		authLog := authInfo(cache, authHeader, cookie, ws, secret, internal)

		if authLog == "" {
			logrus.Infof("%v %v => %v, %v", r.Method, r.RequestURI, record.status, time.Since(t))
		} else {
			logrus.Infof("%v %v => %v, %v, %v", r.Method, r.RequestURI, record.status, time.Since(t), authLog)
		}
	})
}

func authInfo(cache *utils.RequestCache, authHeader, cookie, ws, secret, internal string) string {
	if authHeader == "" && cookie == "" && ws == "" && secret == "" && internal == "" {
		return ""
	}

	if internal != "" {
		return cacheHash(cache, internal, "internal", internal)
	} else if ws != "" && secret != "" {
		key := "ws-secret-" + ws + secret
		val := cache.GetString(key)
		if val == "" {
			val = fmt.Sprintf("%x", sha1.Sum([]byte(secret)))
			cache.SetString(key, val)
		}
		return fmt.Sprintf("[ws=%v,secret=%v]", ws, val)
	} else if authHeader != "" {
		return cacheHash(cache, authHeader, "token", strings.TrimPrefix(authHeader, "Bearer "))
	} else if cookie != "" {
		return cacheHash(cache, cookie, "cookie", cookie)
	}
	return ""
}

func cacheHash(cache *utils.RequestCache, cacheKey, prefix, value string) string {
	key := prefix + "-" + cacheKey
	val := cache.GetString(key)
	if val == "" {
		val = fmt.Sprintf("%x", sha1.Sum([]byte(value)))
		cache.SetString(key, val)
	}
	return "[" + prefix + "=" + val + "]"
}

type ResponseError struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
}

func EntityNotFoundError(req *restful.Request, name string, err error) error {
	return errors.NewStatusReason(
		http.StatusNotFound,
		fmt.Sprintf("%v '%v' not found", strings.Title(currentType(req)), name),
		err.Error(),
	)
}

func AlreadyExistsError(req *restful.Request, name string) error {
	return errors.NewStatus(
		http.StatusConflict,
		fmt.Sprintf("%v '%v' already exists", strings.Title(currentType(req)), name),
	)
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

func NotFoundHandler() http.HandlerFunc {
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

const checkWorkspace = "check-for-auth-workspace"

func (api *API) CheckAuth(method, entityType, authHeader,
	requestWorkspace, cookie, ws, secret string, masterClient io.PlukClient) (bool, error) {
	key := authHeader + requestWorkspace + cookie + ws + secret

	authURL := utils.AuthValidationURL()
	if authURL == "" && !utils.HasMasters() {
		return true, nil
	}

	if api.cache.Get(key) {
		return true, nil
	} else {
		if utils.HasMasters() {
			// Talk to master.
			logrus.Debugf("Auth request to master %v", utils.Masters()[0])
			ws := requestWorkspace
			if ws == "" {
				ws = "kuberlab"
			}
			if masterClient == nil && ws != "" && secret != "" {
				masterClient = plukclient.NewMasterClientWithSecret(ws, secret)
			}
			_, err := masterClient.ListEntities(entityType, ws)
			if err != nil {
				return false, errors.NewStatus(http.StatusUnauthorized, err.Error())
			}
		} else if ws != "" && secret != "" {
			// workspace is empty if we request chunks
			//allow := requestWorkspace == "kuberlab" || requestWorkspace == ""
			//deny := requestWorkspace != ws
			if requestWorkspace != "" {
				if requestWorkspace != ws && requestWorkspace != "kuberlab" {
					if method != http.MethodGet {
						return false, errors.NewStatus(http.StatusForbidden, "Forbidden access to another workspace.")
					}
					// Try request workspace (in case if it is public)
					u, err := url.Parse(authURL)
					if err != nil {
						return false, err
					}
					validationURL := fmt.Sprintf("%v://%v/api/v0.2/workspace/%v", u.Scheme, u.Host, requestWorkspace)
					request, _ := http.NewRequest("GET", validationURL, nil)
					request.Header.Add("Cookie", cookie)
					request.Header.Add("Authorization", authHeader)
					logrus.Debugf("GET %v", request.URL)
					r, err := api.client.Do(request)
					if err != nil {
						return false, err
					}
					logrus.Debugf("Got %v", r.StatusCode)
					if r.StatusCode >= 400 {
						return false, errors.NewStatus(r.StatusCode, fmt.Sprintf("Cannot authenticate to %v", authURL))
					} else {
						wspace := &dealerclient.Workspace{}
						err = json.NewDecoder(r.Body).Decode(wspace)
						if err != nil {
							return false, errors.NewStatus(http.StatusUnauthorized, fmt.Sprintf("Cannot authenticate to %v: %v", authURL, err))
						}
						if len(wspace.Can) == 0 && wspace.Type != "public" {
							return false, errors.NewStatus(http.StatusForbidden, fmt.Sprintf("Cannot authenticate to %v", authURL))
						}
					}
				}
				//logrus.Error(fmt.Sprintf("Invalid auth to %v: workspace and secret don't match.", authURL))
				//WriteErrorString(resp, http.StatusUnauthorized, "Unauthorized.")
				//return
			}
			u, err := url.Parse(authURL)
			if err != nil {
				return false, err
			}
			validationURL := fmt.Sprintf("%v://%v/api/v0.2/secret/%v", u.Scheme, u.Host, secret)
			request, _ := http.NewRequest("GET", validationURL, nil)
			logrus.Debugf("GET %v://%v/[redacted]", request.URL.Scheme, request.URL.Host)
			r, err := api.client.Do(request)
			if err != nil {
				return false, err
			}
			logrus.Debugf("Got %v", r.StatusCode)
			if r.StatusCode >= 400 {
				logrus.Error(fmt.Sprintf("Invalid auth to %v://%v/[redacted]", request.URL.Scheme, request.URL.Host))
				return false, errors.NewStatus(http.StatusUnauthorized, "Unauthorized.")
			}
		} else {
			if requestWorkspace != "" {
				u, err := url.Parse(authURL)
				if err != nil {
					return false, err
				}
				validationURL := fmt.Sprintf("%v://%v/api/v0.2/workspace/%v", u.Scheme, u.Host, requestWorkspace)
				request, _ := http.NewRequest("GET", validationURL, nil)
				request.Header.Add("Cookie", cookie)
				request.Header.Add("Authorization", authHeader)
				logrus.Debugf("GET %v", request.URL)
				r, err := api.client.Do(request)
				if err != nil {
					return false, err
				}
				logrus.Debugf("Got %v", r.StatusCode)
				if r.StatusCode >= 400 {
					return false, errors.NewStatus(r.StatusCode, fmt.Sprintf("Cannot authenticate to %v", authURL))
				} else {
					wspace := &dealerclient.Workspace{}
					err = json.NewDecoder(r.Body).Decode(wspace)
					if err != nil {
						return false, errors.NewStatus(http.StatusUnauthorized, fmt.Sprintf("Cannot authenticate to %v: %v", authURL, err))
					}
					if len(wspace.Can) == 0 {
						return false, errors.NewStatus(http.StatusForbidden, fmt.Sprintf("Cannot authenticate to %v", authURL))
					}
				}

			} else {
				request, _ := http.NewRequest("GET", authURL, nil)
				request.Header.Add("Cookie", cookie)
				request.Header.Add("Authorization", authHeader)

				logrus.Debugf("GET %v", request.URL)

				r, err := api.client.Do(request)
				if err != nil {
					return false, err
				}
				logrus.Debugf("Got %v", r.StatusCode)
				if r.StatusCode >= 400 {
					return false, errors.NewStatus(r.StatusCode, fmt.Sprintf("Cannot authenticate to %v", authURL))
				}
			}
		}
		api.cache.Set(key, true)
	}
	return true, nil
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

	// /pluk/v1/entity-type/workspace/
	splitted := strings.Split(req.Request.URL.Path, "/")
	var requestWorkspace string
	if len(splitted) >= 5 {
		if splitted[3] == "workspaces" {
			requestWorkspace = splitted[4]
		} else if _, ok := plukclient.AllowedTypes[splitted[3]]; ok {
			requestWorkspace = splitted[4]
		}
	}

	masterClient := plukclient.NewMasterClientFromHeaders(req.Request.Header)
	req.SetAttribute("masterclient", masterClient)

	_, err := api.CheckAuth(
		req.Request.Method,
		currentType(req),
		authHeader,
		requestWorkspace,
		cookie,
		ws,
		secret,
		masterClient,
	)
	if err != nil {
		WriteError(resp, err)
		return
	}
	filter.ProcessFilter(req, resp)
}

func setCurrentType(req *restful.Request, resp *restful.Response, filter *restful.FilterChain) {
	resp.PrettyPrint(false)

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

func acquireConcurrency() {
	utils.AcqureSem(1)
}

func releaseConcurrency() {
	utils.ReleaseSem(1)
}
