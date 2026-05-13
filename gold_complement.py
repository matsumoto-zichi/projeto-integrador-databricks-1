import os
from dotenv import load_dotenv
from pyspark.sql.functions import col

load_dotenv()

# envs
CATALOG_NAME = os.environ.get("CATALOG_NAME")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
ENRICHED_METADATA_TABLE = os.environ.get('ENRICHED_METADATA_TABLE')
GOLD_TABLE_COMPLEMENT = os.environ.get('GOLD_TABLE_COMPLEMENT')

# Caminhos
ENRICHED_METADATA_TABLE_PATH = f"{CATALOG_NAME}.{SCHEMA_NAME}.{ENRICHED_METADATA_TABLE}"
GOLD_TABLE_COMPLEMENT_PATH = f"{CATALOG_NAME}.{SCHEMA_NAME}.{GOLD_TABLE_COMPLEMENT}"

if __name__ == "__main__":
    
    print("Lendo os Metadados Enriquecidos (Tabela Limpa)")
    df_metadata = spark.read.table(ENRICHED_METADATA_TABLE_PATH)

    df_metadata.select(
            "sha", "title", "doi", "authors", "publish_time", 
            "url", "full_journal_title", "rank", "paper_br"
        ),

    df_metadata = (df_metadata
                            .select(
                                col("sha").alias("cord_uid"),
                                "title", 
                                "authors", 
                                "doi", 
                                "url", 
                                col("publish_time").alias("dt_publicacao"), 
                                col("full_journal_title").alias("journal_name"), 
                                col("rank").alias("ranking"), 
                                "paper_br"
                            ))
    df_metadata
    print(f"df_gold_chunks: ({df_metadata.count()}, {len(df_metadata.columns)})")

    # Escrita
    df_metadata.write\
        .format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
        .option("delta.enableChangeDataFeed", "true")\
        .saveAsTable(GOLD_TABLE_COMPLEMENT_PATH)

    print("Camada Gold Complementar atualizada.")
