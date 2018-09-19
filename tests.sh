#!/usr/bin/env bash

RED="\033[0;31m"
GREEN="\033[0;32m"
NC='\033[0m' # No color

num=$(go test -v github.com/kuberlab/pluk/... | tee /dev/tty | grep RUN | wc -l)
exit_code=$(echo $?)

if [ $exit_code -eq 0 ]; then
  echo -e "${GREEN}Run $num tests\nTests passed${NC}"
else
  echo -e "${RED}Run $num tests\nTests failed${NC}"
  exit $exit_code
fi
