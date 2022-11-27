#!/bin/bash

docker run -it \
  -e HOST_IP=0.0.0.0 \
  -p 9090:9090 \
  -p 12345:12345 \
  pravega/pravega:latest \
  standalone