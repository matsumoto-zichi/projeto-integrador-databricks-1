# RUNBOOK.md - Projeto de Ingestão e RAG Databricks (CORD-19)

Este documento descreve os procedimentos operacionais padrão (SOP) para preparação, execução, testes e manutenção do pipeline de dados RAG (Retrieval-Augmented Generation) baseado no CORD-19 Research Challenge Dataset.

## 1. Pré-requisitos de Ambiente, Dependências e Ferramentas

### 1.1. Ambientes e Contas
* **Databricks Workspace (Free Edition)**: Acesso ativo com permissões para o Unity Catalog.
* **Kaggle**: Conta ativa para gerar token da API e realizar o download do dataset CORD-19 original (aprox. 87 GB).
* **Ambiente Local/VM**: Para execução de scripts orquestradores (devido a algumas limitações da Free Edition do Databricks).

### 1.2. Ferramentas e Frameworks
* **Processamento de Dados**: Apache Spark (PySpark), Delta Lake, Python local.
* **Gerenciador de Dependências**: requirements.txt.
* **Linguagem**: Python 3.x (Polars, Pandas, LangChain, SentenceTransformers).
* **Busca Vetorial**: Databricks Vector Search.
* **Modelos**: 
  * Embeddings: `all-MiniLM-L6-v2` (Hugging Face / Open Source).

---

## 2. Preparação e Configuração do Ambiente

1. **Clone os Repositórios:**
   Local:
   ```bash
   git clone https://github.com/matsu-zichi/projeto-integrador-databricks
   ```
   Databricks:
   - navegue para o Workspace > Repo
   - clique em "Create" no canto superior direito e crie um folder
   - entre no folder e dentro dele clique em "Create" > "Repo"
   - cole o link e insira as suas credenciais
   - clique em "Open in editor" na parte superior 
2. **Configuração das Variáveis de Ambiente:**
   Copie o arquivo `.env.example` para `.env` na raiz dos projetos e preencha com suas credenciais:
   * `DATABRICKS_HOST` e `DATABRICKS_TOKEN` (Nunca faça *hardcode* das credenciais).
   * Variáveis de volume do Unity Catalog (`CATALOG_NAME`, `SCHEMA_NAME`, etc).
3. **Instalação das Dependências locais:**
   ```bash
   cd projeto-integrador-databricks
   python3 -m venv my_venv
   source my_venv/bin/activate
   which python3
   python3 -m pip install -r requirements.txt
   ```
4. **Instalação das Dependências na Databricks**
   
   Navegue em Environments e selecione o arquivo requirements.txt. e clique em "Apply"

5. **Configuração das variáveis de ambiente**

O projeto utiliza um arquivo `.env` para gerenciar credenciais e constantes de ambiente. Certifique-se de criar o arquivo e preencher as variáveis abaixo antes de iniciar a execução:

Variáveis Databricks:
- CATALOG_NAME: nome do catalogo. Ex.: projeto_integrador_cors19
- SCHEMA_NAME: nome do schema. Ex.: dados_cord19
- VOLUME_BRONZE: nome do volume que armazenará os dados brutos. Ex.: bronze
- VOLUME_SILVER: nome do volume que armazenará os dados limpos. Ex.: silver
- VOLUME_GOLD: nome do volume que armazenará os dados processados. Ex.: gold
- DATABRICKS_HOST: host do projeto na Databricks para execução local
- DATABRICKS_TOKEN: token do usuário no projeto na Databricks para execução local

Variáveis Ingestão Kaggle:
- KAGGLE_USERNAME: nome do usuário na Kaggle
- KAGGLE_KEY: token gerado pelo usuário

Variáveis Enriquecimento dos dados:
- CROSSREF_DATA_PATH: nome da pasta com os dados do [CROSSREF](https://academictorrents.com/details/b5ee0e102689b3e67023dd024694c0f5f124646f) baixados. Ex.: = "C:\Usuários\usuario\Downloads\March 2026 Public Data File from Crossref"
- OUTPUT_CROSSREF_DIR: nome da pasta com os dados do CROSSREF selecionados. Ex.: "C:\Usuários\usuario\Downloads\kaggle-covid-19-data\extracao-crossref"
- OUTPUT_ARTIGOS_BR: nome do arquivo que será salvo com os DOIs dos artigos brasileiros. Ex.:"papers_brasileiros.csv"
- ENRICHED_DOI_JCR: nome do arquivo que será salvo com os DOIs enriquecidos e ISSN padronizados. Ex.: "enriquecimento_doi_jcr.parquet"
- ENRICHED_METADATA_CSV: nome do arquivo CSV que conterá os metadados enriquecidos. Ex.: "metadata_enriched.csv"
- ENRICHED_METADATA_TABLE: nome da tabela criada a partir do csv de metadado enriquecidos. Ex.: "metadata_enriched_cleaned"
- ENRICHED_METADATA_CSV: nome para o arquivo enriquecido final. Ex.: "metadata_enriched.csv"


Variáveis da camada Silver:
- SILVER_JSON_FOLDER: nome da pasta que irá conter os JSONs no volume Silver. Ex.: json_selecionados

Variáveis da camada Gold:
- GOLD_TABLE_COMPLEMENT: nome da tabela que conterá os dados complementares dos artigos. Ex. gold_articles_complement
- GOLD_CHUNCK_PARQUET_FILE: nome do arquivo parquet após chunk dos jsons. Ex. gold_article_chunks.parquet
- GOLD_TABLE_CHUNKS: nome da tabela que conterá os chunks dos jsons. Ex.: gold_articles_chunks
- GOLD_TABLE_EMBEDDINGS: nome da tabela que conterá os embeddings dos chunks. Ex. gold_articles_chunks_embeddings
- GOLD_TABLE_EMBEDDINGS_ENRICHED: nome da tabela que conterá os embeddings e dados enriquecidos. Ex. gold_articles_chunks_embeddings_enriched
- GOLD_TABLE_EMBEDDINGS_ENRICHED_INDEX: nome do index que será criado sobre a tabela de embeddings e dados enriquecidos. Ex.: gold_articles_chunks_embeddings_enriched_index
- GOLD_TABLE_EMBEDDINGS_ENRICHED_VECTOR_COLUMN_NAME: nome da coluna que conterá os vetores. Ex.: chunk_text_vector

Variáveis RAG:
- EMBEDDING_MODEL_NAME: nome do modelo usado para embedding: Ex.: all-MiniLM-L6-v2
- EMBEDDING_MODEL_DIMENSION: quantidade de dimensoes: Ex.: para o all-MiniLM-L6-v2 seria 384
- ENDPOINT_NAME: nome do endpoint para o Vector Search Index. Ex.: vector-search-projeto-integrador

---

## 3. Passo a Passo para Execução do Pipeline

Para reproduzir os resultados, execute os scripts na ordem exata descrita abaixo. Os primeiros passos precisam ser executados localmente devido a limitações de espaço da Free Edition da Databrick. Por conta dessas limitaçoes, não foi possível usar uma ferramenta que orquestrasse a execução dos scripts de forma automática.

### Fase I: Infraestrutura e Ingestão (Bronze)

1. 
**`set_up.py`**: Cria o catálogo, schema e os volumes necessários no Databricks.


2. 
**`ingest_bronze.py`**: Realiza o download dos dados brutos do Kaggle via API e envia para o Volume Bronze. (aqui é **recomendado executar localmente**, uma vez que o arquivo zipado tem 18gb e o Free Edition tem limite de 15gb de espaço)


3. 
- 3.1 - `crossref_extractor.py` (deve ser executado local pela quantidade de dados)
- 3.2 - `papers_br.py` (deve ser executado na Databricks)
- 3.3 - `enrich_dois_jcr.py` -- (deve ser executado local pois precisa dos arquivos do CROSSREF selecionados no crossref_extractor.py)
   - data/jcr.csv. Arquivo de 2.66 mb fornecido pelo professor Leandro e que contém informações complementares sobre os artigos como código ISSN e Journal Impact Factor.
- 3.4 - `crossref_generate_enriched_metadatav.py` -- (deve ser executado na Databricks)


4. `clean_metadata.py`: Padroniza as colunas do arquivo `metadata_enriched.csv`, remove registros sem identificador (SHA) e filtra publicações anteriores a 2020, e salva como uma tabela.

### Fase II: Refinamento e Seleção (Silver) - **A partir daqui, é tudo executado na Databricks**

4. 
**`ingest_silver.py`**: Lê o metadado limpo e move apenas os arquivos JSON selecionados para a camada Silver, garantindo economia de armazenamento e foco na qualidade.


### Fase III: Estruturação e Inteligência (Gold)

5. 
**`gold_complement.py`**: Consolida informações contextuais (Journal, Título, Rank, produção brasileira) em uma tabela de metadados para apoio à resposta da LLM.


6. 
**`gold_chunks_splitter_fixed_size.py`**: Realiza o *chunking* do conteúdo dos artigos em tamanhos fixos para garantir janelas de contexto homogêneas.


7. 
**`create_embeddings.py`**: Calcula os vetores numéricos para cada chunk. Utiliza processamento em lote para evitar erros de memória (OOM).


8. 
**`gold_chunks_final.py`**: Une a tabela de embeddings com os metadados complementares. Esta separação de tarefas protege a memória dos Python Workers ao lidar apenas com os dados necessários para o embedding.



### Fase IV: Busca e Validação

9. 
**`create_vector_search_index.py`**: Cria o índice de busca vetorial sincronizado com a tabela Delta final. Note que há uma limitação de 1 índice por workspace em camadas básicas.


10. 
**`test_retrive_content.py`**: Valida a precisão da recuperação, retornando os 5 trechos de maior similaridade para uma pergunta de teste.


11. 
**`test_llm_response.py`**: Executa o fluxo completo do RAG, gerando uma resposta contextualizada através de um modelo de linguagem (LLM).

---

## 4. Procedimentos de Validação e Testes Básicos

* **Validação de Qualidade de Dados (Silver/Gold)**: Confirmar se há `zero` registros nulos no `paper_id` e nos chunks, além do limite correto dos tokens para inserção no index.
* **Teste do Vetor**: Rodar query de similaridade passando um query_vector no Databricks VectorSearchClient para se certificar de que os metadados e os fragmentos semânticos do JSON estão vindo de forma correta.

---

## 5. Problemas Conhecidos, Limitações e Ações de Contingência

| Problema Conhecido | Causa Raiz Provável | Ação de Contingência / Resolução |
| :--- | :--- | :--- |
| **Erro de Memória OOM (Out Of Memory) no Spark** | Partições muito grandes de JSON sendo vetorizadas de uma só vez. | Editar notebooks de ingestão Gold diminuindo o tamanho da variável `batch_size` (atualmente 100) e/ou aumentar o número de partições no dataframe Spark. |
| **Resultados de Busca Vetorial Irrelevantes** | Chunking inadequado (texto cortado ao meio quebrando semântica). | Ajustar o parâmetro de `overlap` no `RecursiveCharacterTextSplitter` para manter melhor o contexto ou trocar estratégia de separador. |
| **Limitação de DBUs e Throughput (Camada Gold)** | Gargalos devido ao ambiente ser *Free Edition* Workspace. | Executar cálculos de chunking localmente ou em lote incremental. Priorizar scripts abertos Open Source e local storage se o Delta Sync apresentar interrupção de infra. |
| **Prompt Injection ou Retorno Perigoso via LLM** | Insecure output validation pela LLM. | Garantir que a regra do prompt enforce: *"responder apenas com base nos artigos fornecidos"*. Empregar Guardrails (middleware de sanitização de output) antes de imprimir a resposta ao usuário final via Open WebUI. |