#!/usr/bin/env bash

RED="\033[0;31m"
GREEN="\033[0;32m"
NC='\033[0m' # No color

go test -v github.com/kuberlab/pluk/...
exit_code=$(echo $?)

if [ $exit_code -eq 0 ]; then
  echo -e "${GREEN}Tests passed${NC}"
else
  echo -e "${RED}Tests failed${NC}"
  exit $exit_code
fi
