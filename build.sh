#!/usr/bin/env bash

env GOOS=linux go build pluksrv.go && docker build --tag kuberlab/pluk:latest -f Dockerfile .