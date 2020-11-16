#!/bin/bash

# wget -c \
#   https://github.com/ssb-ngi-pointer/ssb-fixtures/releases/download/2.2.0/v2-catamaran-m1000000-a8000.tar.gz \
#   -O - | tar -xz --one-top-level=fixture

wget -c \
  https://github.com/ssb-ngi-pointer/ssb-fixtures/releases/download/2.2.0/v2-sloop-m100000-a2000.tar.gz \
  -O - | tar -xz --one-top-level=fixture

touch fixture/flume/log.bipf

node copy-json-to-bipf-async.js fixture/flume/log.offset fixture/flume/log.bipf
