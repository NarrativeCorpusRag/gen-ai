import os
from typing import Iterator
import pandas as pd
from pymongo import MongoClient, UpdateOne
from dagster import Config
from dagster import ConfigurableResource
import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())

class MongoDBUploadConfig(Config):
    """Configuration for MongoDB upload"""
    mongodb_uri: str = os.getenv("MONGODB_URI")
    database_name: str = "DataScience"
    
    # Input paths from your pipeline
    embeddings_path: str = "gs://gen-ai-tu/news/chunks_with_embeddings/"
    entities_path: str = "gs://gen-ai-tu/news/graph_rag/entities/"
    relations_path: str = "gs://gen-ai-tu/news/graph_rag/relations/"
    
    batch_size: int = 1000

class MongoDBConfig(Config):
    """Configuration for MongoDB connection and collections"""
    connection_string: str = os.getenv("MONGODB_URI")
    database_name: str = "DataScience"
    chunks_collection: str = "chunks"
    entities_collection: str = "entities"
    relations_collection: str = "relations"
    batch_size: int = 1000


class MongoDBResource(ConfigurableResource):
    """Dagster resource for MongoDB operations"""
    connection_string: str = os.getenv("MONGODB_URI")
    database_name: str = 'DataScience'
    
def get_client(self) -> MongoClient:
    return MongoClient(self.connectio_string)
    
def get_database(self):
    client = self.get_client()
    return client[self.database_name]
    

def write_to_mongodb_partition(
    iterator: Iterator[pd.DataFrame],
    mongodb_uri: str,
    database_name: str,
    collection_name: str,
    id_field: str = "_id",
) -> Iterator[pd.DataFrame]:
    """
    Write partition data to MongoDB using bulk upserts.
    Runs on Spark workers.
    """    
    client = MongoClient(mongodb_uri)
    db = client[database_name]
    collection = db[collection_name]
    
    total_written = 0
    
    for document in iterator:
        if document.empty:
            continue
        
        records = document.to_dict('records')
        
        # Batch upsert
        operations = []
        for record in records:
            # Clean up NaN values
            record = {k: v for k, v in record.items() if pd.notna(v)}
            
            # Set _id
            if id_field in record and "_id" not in record:
                record["_id"] = record[id_field]
            
            operations.append(
                UpdateOne(
                    {"_id": record["_id"]},
                    {"$set": record},
                    upsert=True
                )
            )
            
            if len(operations) >= 1000:
                collection.bulk_write(operations, ordered=False)
                total_written += len(operations)
                operations = []
        
        if operations:
            collection.bulk_write(operations, ordered=False)
            total_written += len(operations)
        
        # Yield a summary row
        yield pd.DataFrame([{"records_written": total_written}])
    
    client.close()


def write_to_mongodb_spark_connector(
    df,
    mongodb_uri: str,
    database_name: str,
    collection_name: str,
    mode: str = "overwrite",
):
    """
    Write DataFrame to MongoDB using the official Spark MongoDB Connector.

    More efficient for large datasets than mapInPandas approach.
    """
    df.write \
        .format("mongodb") \
        .mode(mode) \
        .option("connection.uri", mongodb_uri) \
        .option("database", database_name) \
        .option("collection", collection_name) \
        .option("replaceDocument", "true") \
        .option("maxBatchSize", 512) \
        .save()