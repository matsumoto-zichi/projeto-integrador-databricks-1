import polars as pl
import os
import json
import logging
import time
import io
from pathlib import Path
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient


load_dotenv()

# Configurações do Databricks via .env
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

CATALOG_NAME = os.environ.get("CATALOG_NAME")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
VOLUME_BRONZE = os.environ.get("VOLUME_BRONZE")

# Caminho no Volume do Databricks
METADATA_VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_BRONZE}/metadata.csv"

# Configurações locais para o Crossref
CROSSREF_DATA_PATH = os.environ.get("CROSSREF_DATA_PATH")
OUTPUT_CROSSREF_DIR = os.environ.get("OUTPUT_CROSSREF_DIR")


class CrossrefExtractor:
    def __init__(self, input_dir, metadata_path, output_dir):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.metadata_path = metadata_path # String do caminho no Volume
        self.manifest_path = self.output_dir / "pipeline_manifest.json"
        
        # Inicializa o cliente do Databricks SDK
        self.w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
        
        os.makedirs(self.output_dir, exist_ok=True)
        self._setup_logging()
        
        self.doi_df = self._load_doi_df_from_databricks()
        self.processed_files = self._load_manifest()

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler(self.output_dir / "process.log"),
                logging.StreamHandler()
            ]
        )

    def _load_doi_df_from_databricks(self):
        """
        Baixa o metadata.csv do Volume do Databricks e extrai DOIs únicos.
        """
        logging.info(f"Baixando metadados do Databricks: {self.metadata_path}")
        
        try:
            # Baixa o arquivo do Volume para um buffer em memória
            # Nota: Para arquivos muito grandes (>2GB), considere baixar para um arquivo temporário local
            response = self.w.files.download(self.metadata_path)
            file_content = response.contents.read()
            
            # Lê o CSV a partir dos bytes baixados
            df_metadata = pl.read_csv(
                io.BytesIO(file_content),
                columns=["doi"],
                ignore_errors=True
            )
            
            dois_unicos = (
                df_metadata
                .filter(pl.col("doi").is_not_null())
                .select(pl.col("doi").alias("DOI").cast(pl.Utf8).str.strip_chars())
                .unique()
            )
            
            logging.info(f"Total de DOIs únicos carregados do Databricks: {len(dois_unicos)}")
            return dois_unicos

        except Exception as e:
            logging.error(f"Erro ao acessar o Volume do Databricks: {e}")
            raise

    # ... (Os métodos _load_manifest, _save_manifest e process_sequentially permanecem iguais)
    def _load_manifest(self):
        if self.manifest_path.exists():
            with open(self.manifest_path, 'r') as f:
                try: return set(json.load(f))
                except: return set()
        return set()

    def _save_manifest(self):
        with open(self.manifest_path, 'w') as f:
            json.dump(list(self.processed_files), f)

    def process_sequentially(self):
        files = sorted([f for f in self.input_dir.glob("*.jsonl.gz")])
        total_files = len(files)
        logging.info(f"Iniciando extração sequencial: {total_files} arquivos.")

        for i, file_path in enumerate(files, 1):
            file_name = file_path.name
            if file_name in self.processed_files: continue

            start_time = time.time()
            out_path = self.output_dir / f"match_{file_name.replace('.jsonl.gz', '.jsonl')}"

            try:
                df = pl.read_ndjson(file_path, infer_schema_length=None, ignore_errors=True)
                if not df.is_empty():
                    result = df.join(self.doi_df, on="DOI", how="semi")
                    match_count = len(result)
                    if match_count > 0:
                        result.write_ndjson(out_path)
                else:
                    match_count = 0

                duration = time.time() - start_time
                self.processed_files.add(file_name)
                logging.info(f"[{i}/{total_files}] {file_name} | Matches: {match_count} | {duration:.2f}s")

                if i % 20 == 0: self._save_manifest()
            except Exception as e:
                logging.error(f"Erro no arquivo {file_name}: {e}")

        self._save_manifest()

if __name__ == "__main__":
    config = {
        "input_dir": CROSSREF_DATA_PATH,
        "metadata_path": METADATA_VOLUME_PATH,
        "output_dir": OUTPUT_CROSSREF_DIR
    }

    extractor = CrossrefExtractor(**config)
    extractor.process_sequentially()