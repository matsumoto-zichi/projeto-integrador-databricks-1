import os
import glob
import polars as pl
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient


load_dotenv()

# Credenciais e Configurações
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

CATALOG_NAME = os.environ.get("CATALOG_NAME")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
VOLUME_BRONZE = os.environ.get("VOLUME_BRONZE")

ENRICHED_DOI_JCR = os.environ.get("ENRICHED_DOI_JCR")

# Caminhos
OUTPUT_CROSSREF_JSONL = f"{os.environ.get('OUTPUT_CROSSREF_DIR')}/match_*.jsonl"
JCR_PATH = "data/jcr.csv"

# Definimos um caminho local temporário e o caminho remoto no Volume
LOCAL_TEMP_PARQUET = "enriquecimento_doi_jcr_temp.parquet"
REMOTE_VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_BRONZE}/{ENRICHED_DOI_JCR}"

def get_valid_files(pattern):
    all_files = glob.glob(pattern)
    valid_files = []
    corrupted_files = []

    print(f"Verificando integridade de {len(all_files)} arquivos...")
    
    for f in all_files:
        try:
            if os.path.getsize(f) == 0:
                continue
            
            with open(f, 'r') as check_file:
                first_char = check_file.read(1)
                if first_char == '{':
                    valid_files.append(f)
                else:
                    corrupted_files.append(f)
        except Exception:
            corrupted_files.append(f)

    if corrupted_files:
        print(f"AVISO: {len(corrupted_files)} arquivos inválidos ignorados.")
    
    return valid_files

def upload_to_databricks(local_path, remote_path):
    """
    Realiza o upload do arquivo local para o Volume no Databricks via SDK.
    """
    print(f"Iniciando upload para Databricks: {remote_path}...")
    w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
    
    with open(local_path, "rb") as f:
        w.files.upload(remote_path, f, overwrite=True)
    
    print("Upload concluído com sucesso!")

def enrich_data():
    files = get_valid_files(OUTPUT_CROSSREF_JSONL)
    
    if not files:
        print("Erro: Nenhum arquivo JSONL válido encontrado.")
        return

    # 1. Carregar JCR
    df_jcr = pl.read_csv(JCR_PATH).select([
        pl.col("ISSN").str.replace_all("-", "").alias("ISSN_key"),
        pl.col("Full Journal Title"),
        pl.col("Rank"),
        pl.col("Journal Impact Factor")
    ])

    # 2. Pipeline Lazy para processar os 200GB+ de forma eficiente
    pipeline = (
        pl.scan_ndjson(files)
        .select([
            pl.col("DOI"),
            pl.col("ISSN")
        ])
        .drop_nulls("ISSN")
        .explode("ISSN")
        .with_columns(
            pl.col("ISSN").str.replace_all("-", "").alias("ISSN_key")
        )
        .join(df_jcr.lazy(), on="ISSN_key", how="inner")
        .unique(subset=["DOI"])
    )

    print("Executando Join e gerando arquivo local...")
    
    try:
        # Coleta os dados usando engine streaming para não estourar a RAM
        df_final = pipeline.collect(engine="streaming")
        
        # Salva localmente primeiro
        df_final.write_parquet(LOCAL_TEMP_PARQUET)
        
        # Envia para o Databricks Volume
        upload_to_databricks(LOCAL_TEMP_PARQUET, REMOTE_VOLUME_PATH)
        
        # Limpeza: remove o arquivo temporário local
        os.remove(LOCAL_TEMP_PARQUET)
        
        print(f"\nProcesso concluído.")
        print(f"Total de DOIs enriquecidos: {df_final.shape[0]}")
        
    except Exception as e:
        print(f"\nErro crítico: {e}")

if __name__ == "__main__":
    enrich_data()