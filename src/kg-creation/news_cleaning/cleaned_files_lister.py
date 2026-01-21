import gcsfs
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"gen-ai/src/research/jacob/gcp-creds.json"

fs = gcsfs.GCSFileSystem()

# Loop through days 1-30
for day in range(1, 31):
    prefix = f"gs://cleaned-gen-ai-tu/news/raw/year=2025/month=11/day={day:02d}/main_lang=en/"
    print(f"\n{'-'*60}")
    print(f"Day {day:02d}: {prefix}")
    print(f"{'-'*60}")
    try:
        items = fs.ls(prefix, recursive=True)
        print(f"Total items found: {len(items)}")
        for item in items[:50]:  # Show first 50 items per day
            print(f"  {item}")
    except Exception as e:
        print(f"  (No files or error: {e})")