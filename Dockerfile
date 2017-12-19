FROM golang:1.9.2

COPY ./pluksrv.go "$GOPATH/src/github.com/kuberlab/pluk/pluksrv.go"
COPY ./pkg "$GOPATH/src/github.com/kuberlab/pluk/pkg"
COPY ./vendor "$GOPATH/src/github.com/kuberlab/pluk/vendor"

RUN cd "$GOPATH/src/github.com/kuberlab/pluk" && go build pluksrv.go

FROM golang:1.9.2

RUN apt-get update
RUN apt-get install git curl -y

COPY --from=0 /go/src/github.com/kuberlab/pluk/pluksrv /go/bin/pluksrv

CMD [ "pluksrv" ]

EXPOSE 8082