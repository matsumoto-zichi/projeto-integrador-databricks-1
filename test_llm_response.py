import os
from dotenv import load_dotenv
import mlflow.deployments
from databricks.vector_search.client import VectorSearchClient
from sentence_transformers import SentenceTransformer

load_dotenv()

EMBEDDING_MODEL_NAME = os.environ.get("EMBEDDING_MODEL_NAME")
ENDPOINT_NAME = os.environ.get("ENDPOINT_NAME")
GOLD_TABLE_EMBEDDINGS_ENRICHED_INDEX = os.environ.get("GOLD_TABLE_EMBEDDINGS_ENRICHED_INDEX")

print(f"Modelo de Embedding: {EMBEDDING_MODEL_NAME}")
print(f"Endpoint: {ENDPOINT_NAME}")
print(f"Índice: {GOLD_TABLE_EMBEDDINGS_ENRICHED_INDEX}")

client = mlflow.deployments.get_deploy_client("databricks")

# 1. Carregar o modelo localmente (mesmo usado na Gold)
model = SentenceTransformer(EMBEDDING_MODEL_NAME)

# 2. Inicializar o cliente de busca
vsc = VectorSearchClient()

# Verifique se o nome do endpoint e índice batem com sua última criação bem-sucedida
index = vsc.get_index(endpoint_name=ENDPOINT_NAME, index_name=GOLD_TABLE_EMBEDDINGS_ENRICHED_INDEX)


def ask_covid_rag(question):
    # A. Converter pergunta em vetor
    query_vector = model.encode(question).tolist()

    # B. Busca com colunas de metadados enriquecidas
    # Solicitamos as colunas que você mencionou na modelagem Gold
    results = index.similarity_search(
        query_vector=query_vector,
        columns=["chunk_text", "title", "authors", "doi", "dt_publicacao", "journal_name"], 
        num_results=4 # 4 resultados costumam ser ideais para não estourar o contexto da LLM
    )

    # C. Formatação do Contexto Enriquecido
    # Criamos um bloco estruturado para que a LLM saiba "quem disse o quê"
    context_blocks = []
    data = results.get('result', {}).get('data_array', [])

    for res in data:
        text, title, authors, doi, date, journal = res[0], res[1], res[2], res[3], res[4], res[5]
        # Extraímos apenas o ano da data (AAAA-MM-DD)
        year = str(date)[:4] if date else "N/A"
        
        block = f"""
        SOURCE: {title}
        AUTHORS: {authors}
        JOURNAL: {journal} ({year})
        DOI: {doi}
        CONTENT: {text}
        -------------------------"""
        context_blocks.append(block)

    formatted_context = "\n".join(context_blocks)
    
    # D. Prompt com instrução de citação rigorosa
    prompt = f"""You are a specialized medical research assistant. Use the provided context from CORD-19 papers to answer the user's question accurately.
    
    INSTRUCTIONS:
    1. Base your answer ONLY on the context below.
    2. At the end of your answer, provide a "REFERENCES" section listing the Title, Authors, Journal (Year), and DOI for each paper used.
    3. If the context doesn't contain the answer, state that you don't have enough information.

    CONTEXT:
    {formatted_context}
    
    QUESTION: {question}
    
    ANSWER:"""

    # E. Chamada da LLM
    response = client.predict(
        endpoint="databricks-llama-4-maverick", 
        inputs={
            "messages": [
                {"role": "system", "content": "You are a helpful assistant that cites scientific sources accurately."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 1000,
            "temperature": 0.1 # Temperatura baixa para evitar alucinações em dados científicos
        }
    )
    
    return response['choices'][0]['message']['content']

# --- Execução do Teste ---
print("-" * 50)
pergunta = "What are the main discoveries about COVID-19 transmition on closed environments?"
resultado = ask_covid_rag(pergunta)
print(resultado)