from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, IntegerType
)
from dagster_pipes import open_dagster_pipes, PipesContext
from pyspark.sql import functions as F

from dagster_pipes import (
    PipesCliArgsParamsLoader,
    PipesGCSContextLoader,
    PipesGCSMessageWriter,
    open_dagster_pipes,
)
from google.cloud.storage import Client as GCSClient
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass, field
import hashlib
import re
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')

@dataclass
class PreprocessingConfig:
    """Configuration for the preprocessing pipeline"""
    # Language filter
    target_language: str = "en"
    
    # Gopher Quality Filter thresholds (from datatrove)
    min_doc_words: int = 50
    max_doc_words: int = 100_000
    min_avg_word_length: float = 3.0
    max_avg_word_length: float = 10.0
    max_symbol_word_ratio: float = 0.1
    max_bullet_lines_ratio: float = 0.9
    max_ellipsis_lines_ratio: float = 0.3
    max_non_alpha_words_ratio: float = 0.8
    min_stop_words: int = 2
    stop_words: List[str] = field(default_factory=lambda: stopwords.words('english'))
    
    # Gopher Repetition Filter thresholds
    max_dup_line_char_frac: float = 0.3
    max_dup_line_frac: float = 0.3
    max_top_2_gram_frac: float = 0.2
    max_top_3_gram_frac: float = 0.18
    max_top_4_gram_frac: float = 0.16
    
    # C4 Quality Filter thresholds
    min_words_c4: int = 50
    max_words_c4: int = 100_000
    min_sentence_count: int = 3
    curly_bracket_ratio: float = 0.025  # max ratio of { } characters
    
    # Chunking config for RAG
    chunk_size: int = 512  # in words
    chunk_overlap: int = 50  # overlap in words
    
    # Processing
    batch_size: int = 1000
    num_partitions: int = 200

class QualityFilters:
    """
    Implements quality filters similar to datatrove:
    - GopherQualityFilter
    - GopherRepetitionFilter  
    - C4QualityFilter
    - FineWebQualityFilter
    """
    
    def __init__(self, config: PreprocessingConfig):
        self.config = config
    
    @staticmethod
    def count_words(text: str) -> int:
        """Count words in text"""
        if not text:
            return 0
        return len(text.split())
    
    @staticmethod
    def get_lines(text: str) -> List[str]:
        """Split text into lines"""
        if not text:
            return []
        return [line.strip() for line in text.split('\n') if line.strip()]
    
    @staticmethod
    def get_words(text: str) -> List[str]:
        """Get list of words"""
        if not text:
            return []
        return text.split()
    
    @staticmethod
    def get_ngrams(words: List[str], n: int) -> List[str]:
        """Generate n-grams from word list"""
        return [' '.join(words[i:i+n]) for i in range(len(words) - n + 1)]
    
    def gopher_quality_filter(self, text: str) -> Tuple[bool, str]:
        """
        Gopher Quality Filter - based on the Gopher paper quality heuristics
        Returns (passed, reason)
        """
        if not text:
            return False, "empty_text"
        
        words = self.get_words(text)
        n_words = len(words)
        
        # Word count checks
        if n_words < self.config.min_doc_words:
            return False, f"too_few_words:{n_words}"
        if n_words > self.config.max_doc_words:
            return False, f"too_many_words:{n_words}"
        
        # Average word length
        if words:
            avg_word_len = sum(len(w) for w in words) / len(words)
            if avg_word_len < self.config.min_avg_word_length:
                return False, f"avg_word_len_too_short:{avg_word_len:.2f}"
            if avg_word_len > self.config.max_avg_word_length:
                return False, f"avg_word_len_too_long:{avg_word_len:.2f}"
        
        # Symbol to word ratio (excessive symbols)
        symbol_chars = sum(1 for c in text if c in '#@<>{}[]|\\^~`')
        if n_words > 0 and symbol_chars / n_words > self.config.max_symbol_word_ratio:
            return False, "too_many_symbols"
        
        # Bullet lines ratio
        lines = self.get_lines(text)
        if lines:
            bullet_lines = sum(1 for line in lines if line.startswith(('•', '-', '*', '·', '►', '●')))
            if bullet_lines / len(lines) > self.config.max_bullet_lines_ratio:
                return False, "too_many_bullet_lines"
        
        # Ellipsis lines ratio
        if lines:
            ellipsis_lines = sum(1 for line in lines if line.endswith('...') or line.endswith('…'))
            if ellipsis_lines / len(lines) > self.config.max_ellipsis_lines_ratio:
                return False, "too_many_ellipsis_lines"
        
        # Non-alphabetic words ratio
        if words:
            non_alpha_words = sum(1 for w in words if not any(c.isalpha() for c in w))
            if non_alpha_words / n_words > self.config.max_non_alpha_words_ratio:
                return False, "too_many_non_alpha_words"
        
        # Stop words check (document should have some stop words to be natural)
        if words:
            stop_count = sum(1 for w in words if w.lower() in self.config.stop_words)
            if stop_count < self.config.min_stop_words:
                return False, f"too_few_stop_words:{stop_count}"
        
        return True, "passed"
    
    def gopher_repetition_filter(self, text: str) -> Tuple[bool, str]:
        """
        Gopher Repetition Filter - detect repetitive content
        Returns (passed, reason)
        """
        if not text:
            return False, "empty_text"
        
        lines = self.get_lines(text)
        words = self.get_words(text)
        
        if not lines or not words:
            return False, "no_content"
        
        # Duplicate line character fraction
        from collections import Counter
        line_counts = Counter(lines)
        dup_line_chars = sum(len(line) * (count - 1) for line, count in line_counts.items() if count > 1)
        total_chars = len(text)
        if total_chars > 0 and dup_line_chars / total_chars > self.config.max_dup_line_char_frac:
            return False, "duplicate_line_chars"
        
        # Duplicate line fraction
        if len(lines) > 0:
            dup_lines = len(lines) - len(set(lines))
            if dup_lines / len(lines) > self.config.max_dup_line_frac:
                return False, "duplicate_lines"
        
        # Top n-gram fractions
        if len(words) >= 2:
            ngram_2 = self.get_ngrams(words, 2)
            if ngram_2:
                top_2gram_count = Counter(ngram_2).most_common(1)[0][1] if ngram_2 else 0
                if top_2gram_count / len(ngram_2) > self.config.max_top_2_gram_frac:
                    return False, "repetitive_2grams"
        
        if len(words) >= 3:
            ngram_3 = self.get_ngrams(words, 3)
            if ngram_3:
                top_3gram_count = Counter(ngram_3).most_common(1)[0][1] if ngram_3 else 0
                if top_3gram_count / len(ngram_3) > self.config.max_top_3_gram_frac:
                    return False, "repetitive_3grams"
        
        if len(words) >= 4:
            ngram_4 = self.get_ngrams(words, 4)
            if ngram_4:
                top_4gram_count = Counter(ngram_4).most_common(1)[0][1] if ngram_4 else 0
                if top_4gram_count / len(ngram_4) > self.config.max_top_4_gram_frac:
                    return False, "repetitive_4grams"
        
        return True, "passed"
    
    def c4_quality_filter(self, text: str) -> Tuple[bool, str]:
        """
        C4 Quality Filter - based on C4 dataset heuristics
        Returns (passed, reason)
        """
        if not text:
            return False, "empty_text"
        
        words = self.get_words(text)
        n_words = len(words)
        
        # Word count
        if n_words < self.config.min_words_c4:
            return False, f"c4_too_few_words:{n_words}"
        if n_words > self.config.max_words_c4:
            return False, f"c4_too_many_words:{n_words}"
        
        # Sentence count (rough heuristic)
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        if len(sentences) < self.config.min_sentence_count:
            return False, f"too_few_sentences:{len(sentences)}"
        
        # Curly bracket ratio (often indicates code/JSON)
        curly_count = text.count('{') + text.count('}')
        if len(text) > 0 and curly_count / len(text) > self.config.curly_bracket_ratio:
            return False, "too_many_curly_brackets"
        
        # Check for lorem ipsum / placeholder text
        if 'lorem ipsum' in text.lower():
            return False, "lorem_ipsum"
        
        # Check for common boilerplate phrases
        boilerplate_phrases = [
            'javascript is disabled',
            'enable javascript',
            'cookies are disabled',
            'please enable cookies',
            'access denied',
            'page not found',
            '404 error',
            'subscribe to our newsletter',
            'sign up for our newsletter',
        ]
        text_lower = text.lower()
        for phrase in boilerplate_phrases:
            if phrase in text_lower:
                return False, f"boilerplate:{phrase[:20]}"
        
        return True, "passed"
    
    def fineweb_quality_filter(self, text: str) -> Tuple[bool, str]:
        """
        FineWeb Quality Filter - additional heuristics from FineWeb paper
        Returns (passed, reason)
        """
        if not text:
            return False, "empty_text"
        
        lines = self.get_lines(text)
        
        # Check for lines that are just "Read more", "Continue reading", etc.
        nav_patterns = [
            r'^read more\.?$',
            r'^continue reading\.?$',
            r'^click here\.?$',
            r'^share this\.?$',
            r'^follow us\.?$',
            r'^subscribe\.?$',
            r'^sign in\.?$',
            r'^log in\.?$',
        ]
        nav_line_count = 0
        for line in lines:
            for pattern in nav_patterns:
                if re.match(pattern, line.lower().strip()):
                    nav_line_count += 1
                    break
        
        if len(lines) > 0 and nav_line_count / len(lines) > 0.3:
            return False, "too_many_nav_lines"
        
        # Check for excessive short lines (often menus/navigation)
        if lines:
            short_lines = sum(1 for line in lines if len(line.split()) < 3)
            if short_lines / len(lines) > 0.7:
                return False, "too_many_short_lines"
        
        return True, "passed"
    
    def apply_all_filters(self, text: str) -> Tuple[bool, str, Dict[str, str]]:
        """
        Apply all quality filters in sequence
        Returns (passed, final_reason, filter_results_dict)
        """
        filter_results = {}
        
        # Gopher Quality
        passed, reason = self.gopher_quality_filter(text)
        filter_results['gopher_quality'] = reason
        if not passed:
            return False, f"gopher_quality:{reason}", filter_results
        
        # Gopher Repetition
        passed, reason = self.gopher_repetition_filter(text)
        filter_results['gopher_repetition'] = reason
        if not passed:
            return False, f"gopher_repetition:{reason}", filter_results
        
        # C4 Quality
        passed, reason = self.c4_quality_filter(text)
        filter_results['c4_quality'] = reason
        if not passed:
            return False, f"c4_quality:{reason}", filter_results
        
        # FineWeb Quality
        passed, reason = self.fineweb_quality_filter(text)
        filter_results['fineweb_quality'] = reason
        if not passed:
            return False, f"fineweb_quality:{reason}", filter_results
        
        return True, "passed", filter_results

def clean_extracted_text(text: str) -> str:
    """
    Clean extracted text by removing common artifacts
    """
    if not text:
        return ""
    
    # Remove multiple newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    # Remove multiple spaces
    text = re.sub(r'[ \t]+', ' ', text)
    
    # Remove lines that are just navigation/boilerplate
    lines = text.split('\n')
    cleaned_lines = []
    
    skip_patterns = [
        r'^(home|menu|navigation|search|login|sign in|sign up|register)$',
        r'^(share|tweet|pin|email|print)$',
        r'^(facebook|twitter|instagram|linkedin|youtube)$',
        r'^\s*[|•·-]\s*$',
        r'^©.*\d{4}',
        r'^all rights reserved',
    ]
    
    for line in lines:
        line_stripped = line.strip().lower()
        skip = False
        for pattern in skip_patterns:
            if re.match(pattern, line_stripped, re.IGNORECASE):
                skip = True
                break
        if not skip:
            cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines).strip()


def extract_text_with_docling(html: str) -> Optional[str]:
    """
    Extract clean text from HTML using Docling
    Falls back to trafilatura if Docling fails
    """
    if not html or not html.strip():
        return None
    
    try:
        from docling.document_converter import DocumentConverter
        from docling.datamodel.base_models import InputFormat
        
        converter = DocumentConverter(
            allowed_formats=[InputFormat.HTML]
        )
        
        result = converter.convert_string(html, input_format=InputFormat.HTML)
        
        if result and result.document:
            text = result.document.export_to_text()
            return text.strip() if text else None
            
    except Exception as e:
        # Can't use pipes.log here - just print or pass
        print(f"Docling extraction failed: {e}, falling back to trafilatura")
    
    # Fallback to trafilatura
    try:
        from trafilatura import extract
        text = extract(html, include_comments=False, include_tables=True)
        return text.strip() if text else None
    except Exception as e:
        print(f"Trafilatura extraction also failed: {e}")
    
    return None


def process_partition_docling(iterator, config: PreprocessingConfig):
    """
    Process a partition of rows - runs on workers.
    MUST be at module level to be picklable.
    """
    import hashlib
    
    # Initialize filters once per partition
    filters = QualityFilters(config)
    
    for row in iterator:
        try:
            html = row.html
            
            # Extract text
            extracted_text = extract_text_with_docling(html)
            
            if not extracted_text:
                continue
            
            # Clean text
            extracted_text = clean_extracted_text(extracted_text)
            
            if not extracted_text:
                continue
            
            # Apply quality filters
            passed, reason, _ = filters.apply_all_filters(extracted_text)
            
            # Generate doc ID
            doc_id = hashlib.sha256(row.uri.encode()).hexdigest()[:16]
            
            yield {
                'doc_id': doc_id,
                'uri': row.uri,
                'host': row.host,
                'extracted_text': extracted_text,
                'word_count': len(extracted_text.split()),
                'quality_passed': passed,
                'quality_reason': reason,
                'http_date': row.http_date,
                'year': row.year,
                'month': row.month,
                'day': row.day,
            }

        except Exception as e:
            print(f"Error processing row {getattr(row, 'uri', 'unknown')}: {e}")
            continue


def create_processing_schema() -> StructType:
    """Create schema for processed documents"""
    return StructType([
        StructField("doc_id", StringType(), False),
        StructField("uri", StringType(), True),
        StructField("host", StringType(), True),
        StructField("extracted_text", StringType(), True),
        StructField("word_count", IntegerType(), True),
        StructField("quality_passed", BooleanType(), True),
        StructField("quality_reason", StringType(), True),
        StructField("http_date", StringType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True),
    ])


def main():
    gcs_client = GCSClient()
        
    with open_dagster_pipes(
        context_loader=PipesGCSContextLoader(client=gcs_client),
        message_writer=PipesGCSMessageWriter(client=gcs_client),
        params_loader=PipesCliArgsParamsLoader(),
    ) as pipes:
        pipes.log.info("Starting CC-NEWS extract job")
        context = PipesContext.get()
        year = context.get_extra("year")
        month = context.get_extra("month")
        repartition = context.get_extra("repartition")
        out_root = context.get_extra("out_root")
        docs_uri = context.get_extra("docs_uri")
        
        config = PreprocessingConfig(
            target_language="en",
            chunk_size=512,
            chunk_overlap=50,
            num_partitions=repartition,
        )
        pipes.log.info(f"Starting CC-NEWS extract for {year}-{month}")

        spark = SparkSession.builder.appName("CC-NEWS").getOrCreate()
        
        df = (
            spark.read
            .parquet(f"{docs_uri}year={year}/month={month}")
            .filter(F.col("main_lang") == "en")
            .select("uri", "host", "html", "text", "day", "main_lang", "http_date")
            .withColumn("year", F.lit(year))
            .withColumn("month", F.lit(month))
            .repartition(1000)
        )
        
        processed_rdd = df.rdd.mapPartitions(
            lambda it: process_partition_docling(it, config)
        )
        processed_df = spark.createDataFrame(processed_rdd, schema=create_processing_schema())
        
        processed_df = processed_df.cache()
        
        total_processed = processed_df.count()
        passed_count = processed_df.filter(F.col("quality_passed") == True).count()

        pipes.log.info(f"Total processed: {total_processed}")
        pipes.log.info(f"Passed quality filters: {passed_count} ({passed_count/total_processed*100:.1f}%)")
        
        pipes.log.info("Filter rejection breakdown:")
        processed_df.filter(F.col("quality_passed") == False) \
            .groupBy("quality_reason") \
            .count() \
            .orderBy(F.desc("count")) \
            .show(20, truncate=False)
        
        pipes.log.info("Saving processed documents...")

        processed_df\
            .write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .parquet(out_root)
        
        pipes.log.info(f"Saved processed documents to {out_root}")
        pipes.log.info("Job Complete.")
        pipes.report_asset_materialization(
            metadata={
                "year": year,
                "month": month,
                "out_root": out_root,
            }
        )
        pipes.log.info("Done.")
        spark.stop()


if __name__ == "__main__":
    main()