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

"$PY" -m pip install --upgrade --force-reinstall --ignore-installed "omegaconf>=2.3.0"
# 2. Install Python Dependencies
# We add docling and nltk here explicitly if they aren't in your packed environment
"$PY" -m pip install --upgrade pip
"$PY" -m pip install surt docling nltk "pydantic>=2.0"

# 3. Pre-download NLTK Data (Stopwords)
"$PY" -c "import nltk; nltk.download('stopwords', download_dir='$DEST/env/nltk_data')"
# Set environment variable so Spark knows where to look
echo "export NLTK_DATA=$DEST/env/nltk_data" >> "$DEST/env/bin/activate"

# 4. Pre-download Docling Models
# Docling uses the huggingface_hub cache. We warm this up on the master/workers now.
# This runs a dummy conversion to force model download.
"$PY" -c "
import logging
from docling.document_converter import DocumentConverter
# Silence logs for the warm-up
logging.basicConfig(level=logging.ERROR)
print('Warming up Docling models...')
try:
    DocumentConverter()
    print('Docling models downloaded successfully.')
except Exception as e:
    print(f'Warning: Model download failed: {e}')
"

# Sanity check
"$PY" -c "import boto3, surt, docling, nltk; print('deps ok')"

# Ensure the spark user can read the models we just downloaded
chmod -R a+rX "$DEST"
