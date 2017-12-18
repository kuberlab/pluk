FROM golang:1.9.2

RUN apt-get update
RUN apt-get install git curl -y

COPY ./pluk "$GOPATH/bin/pluk"

RUN chmod +x "$GOPATH/bin/pluk"

#RUN mkdir /pacak-git-bare
#RUN mkdir /pacak-git-local
#
#VOLUME ["/pacak-git-bare","/pacak-git-local"]

CMD pluk

EXPOSE 8082