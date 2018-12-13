FROM golang:1.10

COPY ./pluksrv.go "$GOPATH/src/github.com/kuberlab/pluk/pluksrv.go"
COPY ./pkg "$GOPATH/src/github.com/kuberlab/pluk/pkg"
COPY ./cmd "$GOPATH/src/github.com/kuberlab/pluk/cmd"
COPY ./vendor "$GOPATH/src/github.com/kuberlab/pluk/vendor"

RUN cd "$GOPATH/src/github.com/kuberlab/pluk" && go build -ldflags="-s -w" pluksrv.go && go build -ldflags="-s -w" ./cmd/kdataset/

FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install git curl sqlite3 -y

COPY --from=0 /go/src/github.com/kuberlab/pluk/pluksrv /go/bin/pluksrv
COPY --from=0 /go/src/github.com/kuberlab/pluk/kdataset /go/bin/kdataset

VOLUME "/pluk"

CMD [ "pluksrv" ]

EXPOSE 8082
