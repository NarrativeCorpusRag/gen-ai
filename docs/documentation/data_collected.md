# Pipeline explanation

## Data Raw Extraction
Currently pipeline ONLY performs an incremental extraction of "CC-NEWS" (Common Crawl News) data. It streams WARC files directly from AWS S3, processes the HTML content to extract metadata and plain text, and stores the results in Google Cloud Storage (GCS) as partitioned Parquet files.

### Workflow Logic
The script operates in two main phases:
1. Stream & Process (ETL)
The script iterates through the master list of WARC paths provided by Common Crawl. For every file not found in the collected_urls list:
- **Streaming:** Uses boto3 and fastwarc to stream the GZIP content directly from S3 without downloading the full file to disk first.
- **Parsing:** HTML to Text: Uses resiliparse to extract the main article text and DOM tree.
- **Metadata:** Extracts HTTP headers (Date, Last-Modified, Charset).
- **Language Detection:** Uses resiliparse.detect_fast to identify the main_lang and a list of probable languages with confidence scores.
- **URL Normalization:** Generates a Sort-friendly URI (SURT) and extracts the host using tldextract.

2. Load & Partition
Data is aggregated into a Polars DataFrame and written to GCS.
**Format:** Parquet.
**Partitioning:** The data is stored using Hive-style partitioning to optimize downstream query performance.

### Output organization  

#### Partitioning Strategy
The data is written to `gs://gen-ai-tu/news/raw/` with the following directory hierarchy: `.../year={YYYY}/month={MM}/day={DD}/main_lang={LANG}/filename={WARC_FILENAME}/part-*.parquet`

##### Partition Columns:
- year (e.g., "2025")
- month (e.g., "11")
- day (Extracted from the filename timestamp)
- main_lang (e.g., "en", "af", "nl")
- filename (The source WARC file name, ensuring traceability)

##### Schema Definition
The following schema describes the output Parquet files:


| Field Name         | Type         | Description                                        |
|--------------------|--------------|----------------------------------------------------|
| uri                | String       | Original Target URI from the WARC record.          |
| tree               | String       | Raw HTML content.                                  |
| text               | String       | Extracted plain text from the HTML.                |
| main_lang          | String       | The primary language detected (ISO code).          |
| langs              | List(String) | List of all detected potential languages.          |
| confs              | List(Int64)  | Confidence scores corresponding to the langs list. |
| http_date          | String       | The Date header from the HTTP response (ISO 8601). |
| http_last_modified | String       | The Last-Modified header (ISO 8601).               |
| http_charset       | String       | Character set encoding declared in headers.        |
| surt_uri           | String       | Sort-friendly URI for canonicalization.            |
| host               | String       | Extracted hostname/domain.                         |
| filename           | String       | (Partition) Source WARC filename.                  |
| year               | String       | (Partition) Year of capture.                       |
| month              | String       | (Partition) Month of capture.                      |
| day                | String       | (Partition) Day of capture.                        |

##### data sample 
|                uri               |               tree               |                text               | main_lang |        langs       |      confs      |          http_date          | http_last_modified | http_charset |             surt_uri             |        host       |             filename             |  year  | month |  day |
|:--------------------------------:|:--------------------------------:|:---------------------------------:|:---------:|:------------------:|:---------------:|:---------------------------:|:------------------:|:------------:|:--------------------------------:|:-----------------:|:--------------------------------:|:------:|:-----:|:----:|
| "https://www.levels.fyi/nl-nl/c… | "<!DOCTYPE html><html lang="nl-… | "App Downloaden     InloggenReg…  | "af"      | ["af", "nl", "de"] | [308, 311, 316] | "2025-11-01T01:44:44+00:00" | null               | "utf-8"      | "fyi,levels)/nl-nl/companies/in… | "fyi.levels"      | "CC-NEWS-20251101004549-04954.w… | "2025" | "11"  | "01" |
| "https://www.levels.fyi/nl-nl/c… | "<!DOCTYPE html><html lang="nl-… | "App Downloaden     InloggenReg…  | "af"      | ["af", "nl", "de"] | [328, 345, 350] | "2025-11-01T01:45:04+00:00" | null               | "utf-8"      | "fyi,levels)/nl-nl/companies/in… | "fyi.levels"      | "CC-NEWS-20251101004549-04954.w… | "2025" | "11"  | "01" |
| "https://jenkinsdeli.com/menu/g… | "<!doctype html><html prefix="o… | "Skip to Main content     • Jenk… | "af"      | ["af", "fr", "en"] | [448, 450, 453] | "2025-11-01T03:38:26+00:00" | null               | "utf-8"      | "com,jenkinsdeli)/menu/group_81… | "com.jenkinsdeli" | "CC-NEWS-20251101033728-04956.w… | "2025" | "11"  | "01" |
| "https://jenkinsdeli.com/menu/g… | "<!doctype html><html prefix="o… | "Skip to Main content     • Jenk… | "af"      | ["af", "no", "fr"] | [455, 465, 469] | "2025-11-01T03:38:34+00:00" | null               | "utf-8"      | "com,jenkinsdeli)/menu/group_81… | "com.jenkinsdeli" | "CC-NEWS-20251101033728-04956.w… | "2025" | "11"  | "01" |
| "https://proagri.co.za/video-sy… | "<!DOCTYPE html> <html class="a… | " Skip to content ProAgri Logo …  | "af"      | ["af", "no", "da"] | [304, 314, 328] | "2025-11-01T08:41:35+00:00" | null               | "utf-8"      | "za,co,proagri)/video-syngenta-… | "za.co.proagri"   | "CC-NEWS-20251101074035-04959.w… | "2025" | "11"  | "01" |