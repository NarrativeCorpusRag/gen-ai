"""
Universal CC-News Text Cleaner
-----------------------------
Cleans the `text` column from CC-News scraped datasets (Fox, CBS,
Scripps, ABC Local, AP News, Reuters, Yahoo, MSN).

Removes:
- Navigation menus
- Footer menus
- Social media lists
- Recommended articles
- 'IN CASE YOU MISSED IT' blocks
- Repeated category blocks
- Advertisements
- Boilerplate text
- Video promo blocks
- Games / Deals / Newsletter spam

Keeps:
- Headline
- Main article body
- Real paragraphs with verbs + sentence structure

Ideal for:
- ReLiK knowledge graph extraction
- GraphRAG workflows
"""

import pandas as pd
import re
import itertools
import fsspec
import pyarrow.parquet as pq
import pyarrow as pa
from google.cloud import storage
import argparse
import os
import datetime
datetime.datetime.now(datetime.timezone.utc)


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gen-ai/src/research/jacob/gcp-creds.json"

EXPECTED_COLS = ["uri", "host", "year", "month", "day", "text"]

# -------------------------------------------------------------------------
# UNIVERSAL BOILERPLATE PATTERNS
# -------------------------------------------------------------------------

GENERIC_BOILERPLATE = [re.compile(p, re.IGNORECASE) for p in [
    r"^\W*$",
    r"Sign In|Sign Out|Log In|Register",
    r"Subscribe|Newsletter|Follow Us",
    r"Facebook|Twitter|Instagram|YouTube|RSS",
    r"Recommended|Related|More from",
    r"Terms of Service|Terms and Conditions|User Agreement",
    r"Do Not Sell|Privacy|Accessibility",
    r"All rights reserved|©\s*20\d\d",
    r"FactSet|Refinitiv|Lipper|Legal Statement",
    r"Advertisement|Sponsored|Ad Choices",
    r"Loading|Please wait|Continue reading",
    r"Read More|Learn More|Click Here",
    r"Report a typo",
    r"Most Popular|Top Stories|Breaking News",
    r"Recommended Videos|Recommended Articles",
    r"You Might Also Like|More from",
    r"Watch Now|Live Watch|Live Video",
    r"Related Articles|Related Stories",
    r"Local News|National News|World News",
    r"Weather|Traffic|Sports|Opinion|Politics",
    r"Games|Deals|Puzzles|Crossword|Sudoku",
    r"IN CASE YOU MISSED IT",
    r"This material may not be published|broadcast|rewritten|redistributed",
    r"Share this article|Share on Facebook|Share on Twitter",
    r"Powered by .*",
    r"About Us|Contact Us|Advertise with Us",
    r"You can now listen to Fox News articles!",
]]

MENU_LINE = r"^[A-Z][A-Z0-9\s\/&\-\.\|]{6,}$"
TEASER_PATTERN = r"^[A-Z][^:]{1,40}: .+"
TIMESTAMP_TEASER = r"^\s*\d+\s+(mins?|hours?)\s+ago"
BULLET_MENU = r"^\s*•\s"
VERB_PATTERN = r"\b(is|are|was|were|be|been|has|have|had|says|said|reports|reported|states|stated|announced|claims|claimed|warns|warned|told|added|noted|will)\b"

MENU_LINE_RE = re.compile(MENU_LINE)
BULLET_MENU_RE = re.compile(BULLET_MENU)
TEASER_RE = re.compile(TEASER_PATTERN)
VERB_RE = re.compile(VERB_PATTERN, re.IGNORECASE)
TIME_RE = re.compile(TIMESTAMP_TEASER, re.IGNORECASE)

# -------------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------------

def split_lines(text):
    return [line.strip() for line in text.splitlines() if line.strip()]

def is_boilerplate(line):
    if MENU_LINE_RE.match(line):
        return True
    if BULLET_MENU_RE.match(line):
        return True
    if TEASER_RE.match(line) and len(line.split()) < 20:
        return True
    for p in GENERIC_BOILERPLATE:
        if p.search(line):
            return True
    return False

def remove_repeated_lines(lines):
    return list(k for k,_ in itertools.groupby(lines))

def remove_repeated_blocks(lines):
    seen = set()
    out = []
    for ln in lines:
        key = ln.lower()
        if key in seen and len(ln.split()) <= 6:
            continue
        seen.add(key)
        out.append(ln)
    return out

def remove_bullet_articles(lines):
    cleaned = []
    for ln in lines:
        if re.match(BULLET_MENU, ln) and ln.count(" ") < 8:
            continue
        cleaned.append(ln)
    return cleaned

def remove_trailing_teasers(lines):
    for i, ln in enumerate(lines):
        if TIME_RE.search(ln):
            return lines[:i]
    return lines

# -------------------------------------------------------------------------
# PARAGRAPH DETECTION
# -------------------------------------------------------------------------

def is_paragraph(line):
    if len(line) < 40:
        return False
    if not re.search(r"[\.!?]", line):
        return False
    if not VERB_RE.search(line):
        return False
    if is_boilerplate(line):
        return False
    if re.match(MENU_LINE, line):
        return False
    return True

def extract_paragraphs(lines):
    return [ln for ln in lines if is_paragraph(ln)]

# -------------------------------------------------------------------------
# HEADLINE DETECTION
# -------------------------------------------------------------------------

def detect_headline(lines):
    for ln in lines[:10]:
        if len(ln.split()) >= 6 and not is_boilerplate(ln) and not re.match(MENU_LINE, ln):
            return ln
    return None

def clean_text(text):
    if not isinstance(text, str) or len(text) < 10:
        return ""
    lines = split_lines(text)
    lines = remove_repeated_lines(lines)
    lines = remove_bullet_articles(lines)
    lines = [ln for ln in lines if not is_boilerplate(ln)]
    lines = remove_repeated_blocks(lines)
    lines = remove_trailing_teasers(lines)
    headline = detect_headline(lines)
    paragraphs = extract_paragraphs(lines)
    if not paragraphs:
        return ""
    if headline and headline not in paragraphs[0]:
        paragraphs = [headline, ""] + paragraphs
    return "\n".join(paragraphs)

# -------------------------------------------------------------------------
# GCS / FILE HELPERS
# -------------------------------------------------------------------------

def create_bucket_if_missing(gs_bucket_uri, location="US"):
    name = gs_bucket_uri.replace("gs://", "").strip("/")
    client = storage.Client()
    try:
        client.get_bucket(name)
    except Exception:
        client.create_bucket(name, location=location)

def list_files_with_service_account(creds_path, bucket_name, prefix):
    client = storage.Client.from_service_account_json(creds_path)
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [f"gs://{bucket_name}/{b.name}" for b in blobs if b.name.endswith(".parquet")]

def write_log_line(log_path, line):
    fs = fsspec.filesystem("gs")
    if log_path.startswith("gs://"):
        if fs.exists(log_path):
            existing = fs.open(log_path, "r").read()
        else:
            existing = ""
        with fs.open(log_path, "w") as f:
            f.write(existing + line + "\n")
    else:
        with open(log_path, 'a') as f:
            f.write(line + "\n")

def process_file_streaming(input_uri, output_uri, batch_size=1024, text_col="text"):
    seen_uris = set()
    written_rows = 0
    fs = fsspec.filesystem("gs")

    if fs.exists(output_uri):
        print(f"⚠ Skipping {output_uri} (already exists)")
        return "SKIPPED_EXISTS"

    with fs.open(input_uri, "rb") as inf:
        pf = pq.ParquetFile(inf)
        print(f"Cleaning {input_uri}")
        if pf.metadata.num_rows == 0:
            print(f"⚠ Skipping empty file {input_uri}")
            return "EMPTY_INPUT"

        with fs.open(output_uri, "wb") as outf:
            writer = None
            for batch in pf.iter_batches(batch_size=batch_size):
                df_batch = batch.to_pandas()
                keep_cols = [c for c in ['uri', 'host', 'year', 'month', 'day', text_col] if c in df_batch.columns]
                df_batch = df_batch[keep_cols]
                if text_col in df_batch.columns:
                    df_batch = df_batch.dropna(subset=[text_col])
                    df_batch = df_batch[df_batch[text_col].str.len() > 0]
                if "uri" in df_batch.columns:
                    df_batch = df_batch[~df_batch["uri"].isin(seen_uris)]
                    seen_uris |= set(df_batch["uri"])
                if not df_batch.empty and text_col in df_batch.columns:
                    df_batch[text_col] = df_batch[text_col].map(clean_text)
                if df_batch.empty:
                    continue
                df_batch = df_batch.reindex(columns=[c for c in EXPECTED_COLS if c in df_batch.columns])
                table = pa.Table.from_pandas(df_batch)
                if writer is None:
                    writer = pq.ParquetWriter(outf, table.schema, compression="snappy")
                writer.write_table(table)
                written_rows += len(df_batch)
            if writer is not None:
                writer.close()
            print(f"{written_rows} rows written to {output_uri}")
    return "EMPTY_AFTER_CLEAN" if written_rows == 0 else "OK"

# -------------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean parquet(s) in GCS without downloading locally")
    parser.add_argument("--input-file", help="Single gs:// input parquet file")
    parser.add_argument("--input-prefix", help="gs:// prefix to list parquet files")
    parser.add_argument("--start-date", help="Start date YYYY-MM-DD for range processing")
    parser.add_argument("--days", type=int, default=30, help="Number of days to process from start date")
    parser.add_argument("--output-bucket", help="gs://bucket to write cleaned files (will be created if missing)")
    parser.add_argument("--log-path", help="gs:// path for processing log file (defaults to <output-bucket>/processing_logs/cleaned.log)")
    parser.add_argument("--batch-size", type=int, default=1024, help="Row batch size for streaming read")
    args = parser.parse_args()

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    # -----------------------------
    # Single file processing
    # -----------------------------
    if args.input_file:
        if args.output_bucket:
            create_bucket_if_missing(args.output_bucket)

        bucket_name = args.input_file.split("/")[2]
        input_bucket_prefix = f"gs://{bucket_name}/"
        relative = args.input_file.replace(input_bucket_prefix, "").lstrip("/")

        # Build output path using forward slashes only
        bucket = args.output_bucket.rstrip("/")
        relative_cleaned = relative.replace(".parquet", "_cleaned.parquet")
        outp = f"{bucket}/{relative_cleaned}"

        # Process file
        status = process_file_streaming(args.input_file, outp, batch_size=args.batch_size)

        # Log
        logp = args.log_path or f"{bucket}/processing_logs/cleaned.log"
        write_log_line(logp, f"{datetime.datetime.utcnow().isoformat()}\t{args.input_file}\t{outp}\t{status}")

    # -----------------------------
    # Prefix / date-range processing
    # -----------------------------
    elif args.input_prefix and args.start_date:
        create_bucket_if_missing(args.output_bucket)
        start = datetime.datetime.strptime(args.start_date, "%Y-%m-%d").date()
        bucket_name = args.input_prefix.split("/")[2]
        raw_prefix_template = args.input_prefix.replace("{year}", "").replace("{month}", "").replace("{day}", "").rstrip("/")

        for i in range(args.days):
            d = start + datetime.timedelta(days=i)
            prefix = args.input_prefix.replace("{year}", f"{d.year}") \
                                     .replace("{month}", f"{d.month:02d}") \
                                     .replace("{day}", f"{d.day:02d}")
            inner_prefix = "/".join(prefix.split("/")[3:])
            files = list_files_with_service_account(creds_path, bucket_name, inner_prefix)
            print(f"{d.isoformat()} → {len(files)} files")

            for idx, fpath in enumerate(files, 1):
                print(f"   → [{idx}/{len(files)}] {fpath}")
                try:
                    # Build output path (forward slashes)
                    input_bucket_prefix = f"gs://{bucket_name}/"
                    relative = fpath.replace(input_bucket_prefix, "").lstrip("/")
                    relative_cleaned = relative.replace(".parquet", "_cleaned.parquet")
                    outp = f"{args.output_bucket.rstrip('/')}/{relative_cleaned}"

                    # Process file (skip if already exists)
                    status = process_file_streaming(fpath, outp, batch_size=args.batch_size)
                except Exception as e:
                    status = f"ERROR: {e}"

                # Log
                logp = args.log_path or f"{args.output_bucket.rstrip('/')}/processing_logs/cleaned.log"
                write_log_line(logp, f"{datetime.datetime.utcnow().isoformat()}\t{fpath}\t{outp}\t{status}")

            print(f"✔ Completed {d.isoformat()}")
