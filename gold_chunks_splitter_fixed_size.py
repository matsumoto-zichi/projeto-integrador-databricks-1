import os
import pandas as pd
import time
from dotenv import load_dotenv
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pyspark.sql.functions import col, concat_ws, row_number
from pyspark.sql import Window

load_dotenv()

# envs
CATALOG_NAME = os.environ.get("CATALOG_NAME")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
SILVER = os.environ.get('VOLUME_SILVER')
SILVER_JSON_FOLDER = os.environ.get('SILVER_JSON_FOLDER')
GOLD = os.environ.get('VOLUME_GOLD')
GOLD_CHUNCK_PARQUET_FILE = os.environ.get('GOLD_CHUNCK_PARQUET_FILE')
GOLD_TABLE_CHUNKS = os.environ.get('GOLD_TABLE_CHUNKS')

# Caminhos
SILVER_JSON_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{SILVER}/{SILVER_JSON_FOLDER}"
GOLD_VOLUME_PARQUET_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{GOLD}/{GOLD_CHUNCK_PARQUET_FILE}"
CHUNK_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.{GOLD_TABLE_CHUNKS}"


if __name__ == "__main__":
    # Inicializar o splitter uma vez no driver
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=100,
        separators=["\n\n", "\n", ".", " ", ""]
    )
    
    print("Lendo os JSONs brutos")
    raw_jsons = spark.read.option("recursiveFileLookup", "true").json(SILVER_JSON_PATH)
    
    print("Extraindo IDs e Conteúdo")
    processed_df = raw_jsons.select(
        col("paper_id").alias("cord_uid"),
        concat_ws(" ", col("abstract.text")).alias("abstract_text"),
        concat_ws(" ", col("body_text.text")).alias("full_body_text")
    )
    
    processed_df = processed_df.withColumn("chunk_text", concat_ws(" \n ", col("abstract_text"), col("full_body_text")))
    
    # Filtrar apenas documentos com abstract não vazio
    processed_df = processed_df.select("cord_uid", "chunk_text")

    # Adicionar números de linha para processamento por batch
    window_spec = Window.orderBy("cord_uid")
    df_with_row_num = processed_df.withColumn("row_num", row_number().over(window_spec))
    
    # Processar em batches para evitar OOM - batch size reduzido
    batch_size = 10  # Reduzido de 100 para 10 para evitar sobrecarga no createDataFrame
    total_rows = processed_df.count()
    print(f"Total de documentos a processar: {total_rows}")
    
    # Inicializar modo de escrita
    write_mode = "overwrite"  # Primeiro batch sobrescreve
    
    for batch_start in range(0, total_rows, batch_size):
        batch_end = batch_start + batch_size
        print(f"Processando batch: linhas {batch_start} a {batch_end}")
        
        # Filtrar este batch e coletar para o driver
        batch_df = (df_with_row_num
                    .filter((col("row_num") >= batch_start) & (col("row_num") < batch_end))
                    .drop("row_num")
                    .toPandas())
        
        if len(batch_df) == 0:
            print("Batch vazio, pulando...")
            continue
        
        # Aplicar o split de texto no driver
        chunks_list = []
        for _, row in batch_df.iterrows():
            cord_uid = row['cord_uid']
            text = row['chunk_text']
            
            if text:
                chunks = splitter.split_text(text)
                for chunk in chunks:
                    chunks_list.append({
                        'cord_uid': cord_uid,
                        'chunk_text': chunk
                    })
        
        if len(chunks_list) == 0:
            print("Nenhum chunk gerado neste batch, pulando...")
            continue
        
        print(f"Chunks gerados neste batch: {len(chunks_list)}")
        
        # Criar DataFrame pandas com os chunks
        chunks_pdf = pd.DataFrame(chunks_list)
        
        # Converter de volta para Spark DataFrame com retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                spark_batch_df = spark.createDataFrame(chunks_pdf)
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Erro ao criar DataFrame (tentativa {attempt + 1}/{max_retries}), aguardando...")
                    time.sleep(2)
                else:
                    raise e
        
        # Escrever este batch na tabela
        (spark_batch_df.write
            .format("delta")
            .mode(write_mode)
            .option("overwriteSchema", "true")
            .option("delta.enableChangeDataFeed", "true")
            .saveAsTable(CHUNK_TABLE))
        
        print(f"Batch escrito com sucesso: {len(chunks_list)} chunks")
        
        # Após o primeiro batch, mudar para modo append
        write_mode = "append"
        
        # Pequena pausa entre batches para não sobrecarregar o serviço
        time.sleep(0.5)
    
    print(f"Tabela {CHUNK_TABLE} atualizada com sucesso!")
    print("Camada Gold Parquet com Chunking atualizada.")
