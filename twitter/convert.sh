#!/usr/bin/env bash

# https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script
ROOT=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")

set -o errexit
set -o nounset
set -o pipefail

CLUSTER=$1

# cargo install parquet --features=cli
~/.cargo/bin/parquet-fromcsv \
    --csv-compression zstd \
    --input-file "${ROOT}/cluster${CLUSTER}.000.zst" \
    --output-file "${ROOT}/cluster${CLUSTER}.000.parquet" \
    --schema "${ROOT}/trace.schema"
