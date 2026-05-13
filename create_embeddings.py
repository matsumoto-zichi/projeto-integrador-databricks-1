import os
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number
from sentence_transformers import SentenceTransformer

load_dotenv()

# envs
CATALOG_NAME = os.environ.get("CATALOG_NAME")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
TARGET_TABLE_RAG_NAME = os.environ.get("TARGET_TABLE_RAG_NAME")
EMBEDDING_MODEL_NAME = os.environ.get("EMBEDDING_MODEL_NAME")
GOLD_TABLE_CHUNKS = os.environ.get("GOLD_TABLE_CHUNKS")
GOLD_TABLE_EMBEDDINGS = os.environ.get("GOLD_TABLE_EMBEDDINGS")
# Caminhos
RAG_TABLE_PATH = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TARGET_TABLE_RAG_NAME}"
CHUNK_TABLE_PATH = f"{CATALOG_NAME}.{SCHEMA_NAME}.{GOLD_TABLE_CHUNKS}"
EMBEDDING_TABLE_PATH = f"{CATALOG_NAME}.{SCHEMA_NAME}.{GOLD_TABLE_EMBEDDINGS}"


def main():

    # Load model once in the driver
    model = SentenceTransformer(EMBEDDING_MODEL_NAME)

    df_gold_chunks = spark.table(CHUNK_TABLE_PATH)

    print(f"df_gold_chunks: ({df_gold_chunks.count()}, {len(df_gold_chunks.columns)})")

    # Add row numbers for batch processing
    window_spec = Window.orderBy("cord_uid")
    df_with_row_num = df_gold_chunks.withColumn("row_num", row_number().over(window_spec))

    # Process in batches to avoid OOM
    # The embeddings are being generated using `all-MiniLM-L6-v2` which produces 384-dimensional vectors. For 1000 rows, that's:
    # - 1000 rows × 384 floats × 4 bytes = ~1.5 MB just for the embeddings
    # - Plus the original text and other columns
    batch_size = 100
    total_rows = df_gold_chunks.count()

    # Initialize write mode
    write_mode = "overwrite" # First overwrite existing index tables

    for batch_start in range(0, total_rows, batch_size):
        batch_end = batch_start + batch_size
        print(f"Processing batch: rows {batch_start} to {batch_end}")
        
        # Filter this batch and collect to driver
        batch_df = (df_with_row_num
                    .filter((col("row_num") >= batch_start) & (col("row_num") < batch_end))
                    .drop("row_num")
                    .toPandas())
        
        if len(batch_df) == 0:
            print("Empty batch, skipping...")
            continue
        
        # Generate embeddings in the driver
        texts = batch_df['chunk_text'].tolist()
        embeddings = model.encode(texts, show_progress_bar=True)
        
        # Add embeddings to the pandas DataFrame
        batch_df['chunk_text_vector'] = embeddings.tolist()
        
        # Convert back to Spark DataFrame
        spark_batch_df = spark.createDataFrame(batch_df)
        
        # Write this batch to the table
        (spark_batch_df.write
        .format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .saveAsTable(EMBEDDING_TABLE_PATH))
        
        # After first batch, switch to append mode
        write_mode = "append"

if __name__ == "__main__":
    main()
    print("Embeddings gerados e tabela Gold atualizada!")