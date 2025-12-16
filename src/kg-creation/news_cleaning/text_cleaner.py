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
import sys
import itertools

# -------------------------------------------------------------------------
# UNIVERSAL BOILERPLATE PATTERNS
# -------------------------------------------------------------------------

GENERIC_BOILERPLATE = [
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
]

MENU_LINE = r"^[A-Z][A-Z0-9\s\/&\-\.\|]{6,}$"
TEASER_PATTERN = r"^[A-Z][^:]{1,40}: .+"
TIMESTAMP_TEASER = r"^\s*\d+\s+(mins?|hours?)\s+ago"
BULLET_MENU = r"^\s*•\s"
VERB_PATTERN = r"\b(is|are|was|were|be|been|has|have|had|says|said|reports|reported|states|stated|announced|claims|claimed|warns|warned|told|added|noted|will)\b"

# -------------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------------

def split_lines(text):
    return [line.strip() for line in text.splitlines() if line.strip()]

def is_boilerplate(line):
    if re.match(MENU_LINE, line):
        return True
    if re.match(BULLET_MENU, line):
        return True
    if re.match(TEASER_PATTERN, line) and len(line.split()) < 20:
        return True
    for p in GENERIC_BOILERPLATE:
        if re.search(p, line, flags=re.IGNORECASE):
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
        if re.search(TIMESTAMP_TEASER, ln, flags=re.IGNORECASE):
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
    if not re.search(VERB_PATTERN, line, flags=re.IGNORECASE):
        return False
    if is_boilerplate(line):
        return False
    if re.match(MENU_LINE, line):
        return False
    return True

def extract_paragraphs(lines):
    return [ln for ln in lines if is_paragraph(ln)]

# -------------------------------------------------------------------------
# HEADLINE + ARTICLE START
# -------------------------------------------------------------------------

def detect_headline(lines):
    for ln in lines[:10]:
        if len(ln.split()) >= 6 and not is_boilerplate(ln) and not re.match(MENU_LINE, ln):
            return ln
    return None

ARTICLE_START_MARKERS = [
    r"^By [A-Z]",
    r"Published",
    r"Updated",
    r"Posted",
    r"[A-Z].+—",  # NEW YORK —
]

def find_article_start(lines):
    for i, ln in enumerate(lines):
        for m in ARTICLE_START_MARKERS:
            if re.search(m, ln):
                return i
    return 0

def normalize(text):
    return re.sub(r"\s+", " ", text).strip()

# -------------------------------------------------------------------------
# CLEAN PIPELINE
# -------------------------------------------------------------------------

def clean_text(text):
    if not isinstance(text, str) or len(text) < 10:
        return ""

    lines = split_lines(text)
    lines = remove_repeated_lines(lines)
    lines = remove_bullet_articles(lines)
    lines = [ln for ln in lines if not is_boilerplate(ln)]
    lines = remove_repeated_blocks(lines)
    lines = remove_trailing_teasers(lines)

    # headline detection
    headline = detect_headline(lines)

    # paragraph extraction
    paragraphs = extract_paragraphs(lines)

    if not paragraphs:
        return ""

    # prepend headline if valid
    if headline and headline not in paragraphs[0]:
        paragraphs = [headline, ""] + paragraphs

    return "\n".join(paragraphs)

# -------------------------------------------------------------------------
# I/O WRAPPER
# -------------------------------------------------------------------------

def main(input_parquet, text_col="text"):
    df = pd.read_parquet(input_parquet, engine="pyarrow")

    # Keep only required columns
    df = df[['uri', 'host', 'year', 'month', 'day', text_col]]

    print(f"Cleaning {len(df)} CC-News documents...")

    # Deduplicate by URI
    df = df.drop_duplicates(subset=["uri"])
    print(f"➡ After dedupe: {len(df)} unique URIs")
    
    # Remove empty rows
    df = df.dropna(subset=[text_col])
    print(f"➡ After removing empty rows: {len(df)} rows")
    
    # Clean text
    df[text_col] = df[text_col].apply(clean_text)

    output = input_parquet.replace(".parquet", "_cleaned.parquet")
    df.to_parquet(output)

    print(f"✔ CLEANED file saved to: {output}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python text_cleaner.py input.parquet")
        sys.exit(1)
    main(sys.argv[1])
