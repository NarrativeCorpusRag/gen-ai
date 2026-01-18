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
from typing import Optional, List, Dict, Tuple, Any
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

def chunk_text_for_rag(
        text: str, 
        chunk_size: int = 512, 
        overlap: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Split text into chunks suitable for RAG embedding
        Uses sentence-aware splitting to maintain context
        
        Returns list of dicts with chunk_text and metadata
        """
        if not text:
            return []
        
        # Split into sentences
        sentence_pattern = r'(?<=[.!?])\s+(?=[A-Z])'
        sentences = re.split(sentence_pattern, text)
        
        chunks = []
        current_chunk = []
        current_word_count = 0
        
        for sentence in sentences:
            sentence_words = len(sentence.split())
            
            if current_word_count + sentence_words <= chunk_size:
                current_chunk.append(sentence)
                current_word_count += sentence_words
            else:
                # Save current chunk
                if current_chunk:
                    chunk_text = ' '.join(current_chunk)
                    chunks.append({
                        'chunk_text': chunk_text,
                        'chunk_word_count': current_word_count,
                    })
                
                # Start new chunk with overlap
                if overlap > 0 and current_chunk:
                    # Take last few sentences for overlap
                    overlap_text = ' '.join(current_chunk)
                    overlap_words = overlap_text.split()
                    if len(overlap_words) > overlap:
                        overlap_start = ' '.join(overlap_words[-overlap:])
                        current_chunk = [overlap_start, sentence]
                        current_word_count = overlap + sentence_words
                    else:
                        current_chunk = [sentence]
                        current_word_count = sentence_words
                else:
                    current_chunk = [sentence]
                    current_word_count = sentence_words
        
        # Don't forget last chunk
        if current_chunk:
            chunk_text = ' '.join(current_chunk)
            chunks.append({
                'chunk_text': chunk_text,
                'chunk_word_count': current_word_count,
            })
        
        # Add chunk indices
        for i, chunk in enumerate(chunks):
            chunk['chunk_index'] = i
        
        return chunks
def chunk_partition(iterator, config: PreprocessingConfig):
        """
        Chunk documents for RAG
        This runs on worker nodes via mapPartitions
        """
        for row in iterator:
            try:
                if not row.quality_passed:
                    continue
                
                chunks = chunk_text_for_rag(
                    row.extracted_text,
                    chunk_size=config.chunk_size,
                    overlap=config.chunk_overlap
                )
                
                for chunk in chunks:
                    chunk_id = hashlib.sha256(
                        f"{row.doc_id}_{chunk['chunk_index']}".encode()
                    ).hexdigest()[:16]
                    
                    yield {
                        'chunk_id': chunk_id,
                        'doc_id': row.doc_id,
                        'chunk_index': chunk['chunk_index'],
                        'chunk_text': chunk['chunk_text'],
                        'chunk_word_count': chunk['chunk_word_count'],
                        'uri': row.uri,
                        'host': row.host,
                        'http_date': row.http_date,
                        'year': row.year,
                        'month': row.month,
                        'day': row.day,
                    }
                    
            except Exception as e:
                print(f"Error chunking document: {e}")
                continue

def create_chunk_schema() -> StructType:
        """Create schema for chunked documents (for RAG)"""
        return StructType([
            StructField("chunk_id", StringType(), False),
            StructField("doc_id", StringType(), False),
            StructField("chunk_index", IntegerType(), True),
            StructField("chunk_text", StringType(), True),
            StructField("chunk_word_count", IntegerType(), True),
            
            # Metadata from parent doc
            StructField("uri", StringType(), True),
            StructField("host", StringType(), True),
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
        # parameters still come from argparse (your current approach), OR from pipes.get_params()
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
            .filter(F.col("quality_passed") == True)
            .withColumn("year", F.lit(int(year)))
            .withColumn("month", F.lit(int(month)))
            .select("uri", "host", "html", "text", "year", "month", "day", "main_lang", "http_date")
            .repartition(config.num_partitions) )

        chunked_rdd = df.rdd.mapPartitions(
                lambda it: chunk_partition(it, config)
        )
            
        chunked_df = spark.createDataFrame(chunked_rdd, schema=create_chunk_schema())
        chunked_df.show()
        chunk_count = chunked_df.count()
        pipes.log.info(f"Total chunks created: {chunk_count}")
            
            # ========================================
            # STAGE 4: Save Chunked Documents
            # ========================================
        pipes.log.info("Saving chunked documents for RAG...")
            
        chunks_output = f"{out_root}/chunks/"
        chunked_df.write \
                .mode("overwrite") \
                .partitionBy("year", "month") \
                .parquet(chunks_output)
            
        pipes.log.info(f"Saved chunks to {chunks_output}")
            
            # Also save as JSONL for easy ingestion into vector DBs
        jsonl_output = f"{out_root}/chunks_jsonl/"
        chunked_df.coalesce(100).write \
            .mode("overwrite") \
            .json(jsonl_output)
            
        pipes.log.info(f"Saved JSONL to {jsonl_output}")
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