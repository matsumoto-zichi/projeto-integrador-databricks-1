import os
from dotenv import load_dotenv
load_dotenv()

# envs
CATALOG_NAME = os.environ.get("CATALOG_NAME")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
GOLD_TABLE_EMBEDDINGS = os.environ.get("GOLD_TABLE_EMBEDDINGS")
GOLD_TABLE_EMBEDDINGS_ENRICHED = os.environ.get("GOLD_TABLE_EMBEDDINGS_ENRICHED")
GOLD_TABLE_COMPLEMENT = os.environ.get("GOLD_TABLE_COMPLEMENT")

# Caminhos
GOLD_TABLE_COMPLEMENT_PATH = f"{CATALOG_NAME}.{SCHEMA_NAME}.{GOLD_TABLE_COMPLEMENT}"
GOLD_TABLE_EMBEDDINGS_PATH = f"{CATALOG_NAME}.{SCHEMA_NAME}.{GOLD_TABLE_EMBEDDINGS}"
GOLD_TABLE_EMBEDDINGS_ENRICHED_DESTINANTION_PATH = f"{CATALOG_NAME}.{SCHEMA_NAME}.{GOLD_TABLE_EMBEDDINGS_ENRICHED}"


def main():

    df_gold_complement = spark.table(GOLD_TABLE_COMPLEMENT_PATH)

    df_gold_embeddings = spark.table(GOLD_TABLE_EMBEDDINGS_PATH)


    df_final_enriched = df_gold_embeddings.join(
        df_gold_complement.drop("chunk_text"), # Removemos o texto para não duplicar no join
        on="cord_uid",
        how="inner"
    )


    (df_final_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(GOLD_TABLE_EMBEDDINGS_ENRICHED_DESTINANTION_PATH)
    )

    print("Sucesso! Embeddings gerados com metadados preservados.")


if __name__ == "__main__":
    main()