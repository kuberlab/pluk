#!/usr/bin/env bash

env GOOS=linux go build pluk.go && docker build --tag kuberlab/pluk:latest -f Dockerfile .