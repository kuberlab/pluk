#!/usr/bin/env bash

protoc protos/pluke.proto --go_out=plugins=grpc:pkg/grpc && mv pkg/grpc/protos/pluke.pb.go pkg/grpc/ && rm -rf pkg/grpc/protos
