#!/usr/bin/env bash
set -euxo pipefail
# Where we want the environment installed on each node
DEST=/opt/gen-ai-env
mkdir -p "$DEST"
cd "$DEST"

# Fetch the self-extracting environment.sh from GCS
gsutil cp gs://gen-ai-tu/artifacts/environment.sh ./environment.sh
chmod +x ./environment.sh

# Unpack (creates ./env and ./activate.sh)
./environment.sh

# Install into the pixi-packed environment (NOT system python)
PY="$DEST/env/bin/python"

"$PY" -m pip install --upgrade pip
"$PY" -m pip install surt

# Sanity check
"$PY" -c "import boto3, surt; print('deps ok')"

chmod -R a+rX "$DEST"