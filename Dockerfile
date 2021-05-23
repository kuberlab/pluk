FROM golang:1.14

COPY ./pluksrv.go "$GOPATH/src/github.com/kuberlab/pluk/pluksrv.go"
COPY ./pkg "$GOPATH/src/github.com/kuberlab/pluk/pkg"
COPY ./cmd "$GOPATH/src/github.com/kuberlab/pluk/cmd"
COPY ./vendor "$GOPATH/src/github.com/kuberlab/pluk/vendor"

RUN cd "$GOPATH/src/github.com/kuberlab/pluk" && \
  go build -tags=jsoniter -ldflags="-s -w" pluksrv.go && \
  go build -ldflags="-s -w" ./cmd/kdataset/

FROM ubuntu:20.04

RUN apt-get update && apt-get install curl sqlite3 -y && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

VOLUME "/pluk"
EXPOSE 8082

COPY --from=0 /go/src/github.com/kuberlab/pluk/pluksrv /usr/bin/pluksrv
COPY --from=0 /go/src/github.com/kuberlab/pluk/kdataset /usr/bin/kdataset

CMD [ "pluksrv" ]
