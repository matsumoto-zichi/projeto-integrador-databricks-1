# Projeto Integrador: CORD-19 RAG para Descoberta de Conhecimento na Base CORD-19 da Kaggle

## 1. Identificação do Grupo
**Grupo C**
* Fábio Luiz Batista de Araujo
* Keren Suzana Bernal Moreno
* Matheus Pereira Nascimento
* Thiago Matsumoto Zichi

---

## 2. Descrição do Problema
Durante a pandemia de COVID-19, observou-se um crescimento exponencial e acelerado da produção científica global sobre o coronavírus. Esse volume massivo de publicações — compilado no *CORD-19 Research Challenge Dataset* com aproximadamente 87 GB de arquivos — tornou extremamente difícil para pesquisadores e profissionais de saúde identificarem rapidamente evidências relevantes, tendências e protocolos clínicos atualizados. O desafio central é extrair e consultar, de maneira eficiente, dados de texto não estruturados nesse imenso repositório de conhecimento.

---

## 3. Objetivos do Trabalho
O objetivo central deste projeto é transformar a base de dados não estruturados do CORD-19 em um sistema de conhecimento consultável utilizando o paradigma *Retrieval-Augmented Generation* (RAG). 

Entre os objetivos específicos, busca-se capacitar o modelo de linguagem (LLM) para responder a questões complexas de pesquisa, tais como:
-- colocar novas perguntas aqui

---

## 4. Arquitetura Desenvolvida e Tecnologias
O projeto segue o paradigma **Lakehouse** e a **Medallion Architecture**, utilizando um fluxo contínuo de refinamento de dados para integração final com busca vetorial.

### 4.1. Camadas de Dados
* **Bronze (Landing Zone):** Armazena os dados brutos (JSONs e CSVs originais do Kaggle e outros complementos) geridos via *Unity Catalog Volumes*.
* **Silver (Trusted):** Dados limpos, desduplicados e filtrados a partir de regras de negócio. Foi realizado um enriquecimento cruzando os metadados com as bases do *JCR (Journal Citation Reports)*, extraindo os top 10.000 globais e os artigos brasileiros (BR).
* **Gold (Delivery):** Textos segmentados (*chunking*) usando `RecursiveCharacterTextSplitter` e transformados em *embeddings* vetoriais de alta dimensão, salvos em arquivos Parquet e tabelas *Delta Lake*.

### 4.2. Fluxo de RAG
A aplicação utiliza as seguintes inovações no agente de IA:
* **Databricks Vector Search:** Criação e consulta do índice vetorial para busca semântica.
* **Raciocínio Chain-of-Thought (CoT):** Obriga o LLM a extrair entidades e avaliar a lógica dos trechos recuperados internamente, antes de gerar a resposta ao usuário.

### 4.3. Principais Tecnologias Utilizadas
* **Processamento e Armazenamento:** Apache Spark (PySpark), Delta Lake, Databricks Unity Catalog.
* **Modelo sentence-transformers:** Modelos de Embedding Hugging Face (`all-MiniLM-L6-v2`).
* **Bibliotecas Python:** `KaggleHub`, `LangChain`, `Polars`, `Pandas`, `SentenceTransformers`, `Databricks Vectorsearch`, `Databricks SDK`. 
* **Gerenciamento de Pacotes e Setup:** Python venv com requirements.txt (padrão no cluster serverless da Databricks).

---

## 5. Organização do Repositório
A estrutura base do código do processamento encontra-se da seguinte forma:

```text
cord19/
├── set_up.py                                   # Configuração do Unity Catalog (local ou Databricks)
├── ingest_bronze.py                            # Ingestão dos dados da Kaggle via API do KaggleHub (local)
├── crossref_extractor.py                       # Processsamento dos dados do Crossref pegando todos os dados dos DOIs localizados em metadata.csv (local)
├── enrich_dois_jcr.py                          # Enriquecimento dos dados do arquivo JCR e Crossref para padronização dos ISSN (local)
├── papers_br.py                                # Processamento dos JSON para verificar quais são feitos por brasileiros (Databricks)
├── crossref_generate_enriched_metadata.py      # Enriquecimento dos dados do metadata.csv com JCR + Crossref e papers brasileiros (local)
├── clean_metadata.py                           # Limpeza e padronização do arquivo de metadata enriquecido (Databricks)
├── ingest_silver.py                            # Ingestão dos JSONs da camada bronze para a silver, selecionando pelo rank do JCR e se é brasileiro - precisa do arquivo enriquecido (Databricks)
├── gold_complement.py                          # Criação da tabela com dados complementares dos artigos como autores, titulo, journal, etc. (Databricks)
├── gold_chunks_splliter_fixed_size.py          # Criação dos chunks do conteúdo dos JSONs da camada silver, salvando em parquet e numa tabela delta (Databricks)
├── create_embeddings.py                        # Criação dos embeddings sob os chunks da tabela da etapa anterior (Databricks)
├── create_vector_search_index.py               # Criação do indice vetorial sob a coluna de embedding da etapa anterior. Obs.: processo demora alguns minutos (Databricks)
├── test_retrive_content.py                     # Teste de recuperação de conteúdo, convertendo uma pergunta usando o mesmo modelo de embedding (Databricks)
├── test_llm_response.py                        # Teste de resposta de uma LLM usando a recuperação de conteúdo (Databricks)
├── requirements.txt                            # Pacotes utilizado no projeto
└── README.md                                   # Documentação principal
```

---

## 6. Instruções de Instalação e Configuração

**Pré-requisitos:** Conta no *Databricks Workspace*, Docker instalado na máquina local, Python 3.10+ e gerenciadores de dependência (`uv` ou `Poetry`).

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
---

## 7. Execução do Pipeline

Para mais detalhes da execução do pipeline, referencie o arquivo de RUNBOOK.md.
