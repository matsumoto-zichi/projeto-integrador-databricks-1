import os
from dotenv import load_dotenv
from databricks.vector_search.client import VectorSearchClient
from sentence_transformers import SentenceTransformer

load_dotenv()

EMBEDDING_MODEL_NAME = os.environ.get("EMBEDDING_MODEL_NAME")
ENDPOINT_NAME = os.environ.get("ENDPOINT_NAME")
GOLD_TABLE_EMBEDDINGS_ENRICHED_INDEX = os.environ.get("GOLD_TABLE_EMBEDDINGS_ENRICHED_INDEX")

print(f"Modelo: {EMBEDDING_MODEL_NAME}")
print(f"Endpoint: {ENDPOINT_NAME}")
print(f"Índice: {GOLD_TABLE_EMBEDDINGS_ENRICHED_INDEX}")

# 1. Carregar o modelo localmente no notebook (o mesmo usado na Gold)
model = SentenceTransformer(EMBEDDING_MODEL_NAME)

# 2. Inicializar o cliente de busca
vsc = VectorSearchClient()
index = vsc.get_index(endpoint_name=ENDPOINT_NAME, index_name=GOLD_TABLE_EMBEDDINGS_ENRICHED_INDEX)

# 3. A pergunta do usuário
user_query = "What are the neurological symptoms of COVID-19?"

# 4. PASSO CHAVE: Converter a pergunta em vetor (Embedding)
# O model.encode retorna um array numpy, precisamos converter para lista
query_vector = model.encode(user_query).tolist()

# 5. Fazer a busca usando query_vector em vez de query_text
results = index.similarity_search(
  query_vector=query_vector,
  columns=["chunk_text", "cord_uid"], # Retorne o ID para citação posterior
  num_results=5
)

# Visualizando os resultados
import json
print(json.dumps(results, indent=2))