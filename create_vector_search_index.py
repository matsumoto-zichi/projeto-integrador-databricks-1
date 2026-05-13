import os
import time
from dotenv import load_dotenv
from databricks.vector_search.client import VectorSearchClient

load_dotenv()

# envs
CATALOG_NAME = os.environ.get("CATALOG_NAME")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
ENDPOINT_NAME = os.environ.get("ENDPOINT_NAME")
GOLD_TABLE_EMBEDDINGS_ENRICHED = os.environ.get("GOLD_TABLE_EMBEDDINGS_ENRICHED")
GOLD_TABLE_EMBEDDINGS_ENRICHED_INDEX = os.environ.get("GOLD_TABLE_EMBEDDINGS_ENRICHED_INDEX")
EMBEDDING_MODEL_DIMENSION = os.environ.get("EMBEDDING_MODEL_DIMENSION")
GOLD_TABLE_EMBEDDINGS_ENRICHED_VECTOR_COLUMN_NAME = os.environ.get("GOLD_TABLE_EMBEDDINGS_ENRICHED_VECTOR_COLUMN_NAME")

# Caminhos
SOURCE_TABLE = f"{CATALOG_NAME}.{CATALOG_NAME}.{GOLD_TABLE_EMBEDDINGS_ENRICHED}"
INDEX_NAME = f"{CATALOG_NAME}.{CATALOG_NAME}.{GOLD_TABLE_EMBEDDINGS_ENRICHED_INDEX}"

def main():

    vsc = VectorSearchClient()


    # 1. Listar todos os endpoints de busca vetorial
    endpoints = vsc.list_endpoints()

    if not endpoints.get('endpoints'):
        print("Nenhum endpoint de Vector Search encontrado para deletar.")
    else:
        for ep in endpoints['endpoints']:
            name = ep['name']
            print(f"Deletando endpoint de busca vetorial: {name}...")
            try:
                vsc.delete_endpoint(name=name)
                print(f"Endpoint '{name}' deletado com sucesso.")
            except Exception as e:
                print(f"Erro ao deletar '{name}': {e}")
        
        # Aguarda alguns segundos para o Databricks processar a liberação da cota
        print("Aguardando 10 segundos para liberação da cota...")
        time.sleep(10)

    # 2. Garantir que o Endpoint existe
    try:
        vsc.get_endpoint(name=ENDPOINT_NAME)
        print(f"Endpoint '{ENDPOINT_NAME}' já existe.")
    except Exception:
        print(f"Criando novo endpoint: {ENDPOINT_NAME}...")
        vsc.create_endpoint(name=ENDPOINT_NAME, endpoint_type="STANDARD")

    # 3. Criar o Índice Delta Sync (Sincronizado com sua tabela Gold)
    # Como você já calculou os embeddings manualmente para evitar OOM, 
    # usamos 'embedding_vector_column' em vez de 'embedding_source_column'.
    vsc.create_delta_sync_index(
        ENDPOINT_NAME=ENDPOINT_NAME,
        SOURCE_TABLE_name=SOURCE_TABLE,
        INDEX_NAME=INDEX_NAME,
        pipeline_type='TRIGGERED',
        primary_key="cord_uid",
        embedding_vector_column=GOLD_TABLE_EMBEDDINGS_ENRICHED_VECTOR_COLUMN_NAME # Nome da coluna que salvamos no passo anterior
        embedding_dimension=EMBEDDING_MODEL_DIMENSION    # Dimensão do modelo all-MiniLM-L6-v2
    )

    print(f"Índice '{INDEX_NAME}' está sendo criado. Esse processo pode levar alguns minutos.")

if __name__ == "__main__":
    main()