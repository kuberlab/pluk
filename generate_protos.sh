#!/usr/bin/env bash

go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

protoc --proto_path=protos --go_out=pkg/grpc --go_opt=paths=source_relative \
 --go-grpc_out=pkg/grpc --go-grpc_opt=paths=source_relative pluke.proto
