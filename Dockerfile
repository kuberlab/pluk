FROM golang:1.9.2

RUN apt-get update
RUN apt-get install git curl -y

COPY ./pluksrv "$GOPATH/bin/pluksrv"

RUN chmod +x "$GOPATH/bin/pluksrv"

#RUN mkdir /pacak-git-bare
#RUN mkdir /pacak-git-local
#
#VOLUME ["/pacak-git-bare","/pacak-git-local"]

CMD pluksrv

EXPOSE 8082