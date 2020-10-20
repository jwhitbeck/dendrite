#!/bin/bash

set -eu -o pipefail

lein uberjar

native-image \
  -cp target/dendrite.cli-0.1.0-SNAPSHOT-standalone.jar \
  --no-server \
  --no-fallback \
  --initialize-at-build-time \
  --report-unsupported-elements-at-runtime \
  -H:Name=den \
  dendrite.cli.core
