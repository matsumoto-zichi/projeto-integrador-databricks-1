import os
import shutil
import zipfile
import kagglehub
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

load_dotenv()

KAGGLE_USERNAME = os.environ.get("KAGGLE_USERNAME")
KAGGLE_KEY = os.environ.get("KAGGLE_KEY")

CATALOG_NAME = os.environ.get("CATALOG_NAME")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
VOLUME_BRONZE = os.environ.get("VOLUME_BRONZE")

w = WorkspaceClient()

OUTPUT_ZIPS_PATH = Path.home() / "Downloads" / "cord19_to_upload"
OUTPUT_ZIPS_PATH.mkdir(parents=True, exist_ok=True)
DATASET_HANDLE = "allen-institute-for-ai/CORD-19-research-challenge"


def metadata_csv_file(metadata_path) -> pd.DataFrame:
    print("\nLendo metadata.csv...")
    df = pd.read_csv(metadata_path, low_memory=False, usecols=["sha", "publish_time"])
    # Garantir que a coluna de data esteja no formato correto e filtrar
    df['publish_time'] = pd.to_datetime(df['publish_time'], errors='coerce')
    # filtra apenas 2020 em diante
    df = df.loc[df['publish_time'].dt.year >= 2020]
    # Cria coluna de ano
    df["year"] = df['publish_time'].dt.year
    # O metadata pode ter SHAs concatenados por '; ', pegamos o primeiro
    df = df.loc[df["sha"].isin(df['sha'].dropna().str.split('; ').str[0])]
    df = df.drop_duplicates()
    return df

def assert_uc_components(item_category: str, item_name: str):
    map_items = {
        "catalog": w.catalogs.get,
        "schema": w.schemas.get,
        "volume": w.volumes.read
    }

    try:
        map_items.get(item_category)(item_name)
    except Exception as e:
        return False
    return True


def map_year_files(needed_shas, json_folder_path):
    print("\nMapeando arquivos...")
    files_by_year = defaultdict(set)

    for _, row in needed_shas.iterrows():
        year = row['year']
        try:
            sha_list = row['sha'].split('; ') # Pode haver múltiplos SHAs por linha
        except Exception as e:
            print(e)
            print(sha)
            raise ValueError("")
        if year not in files_by_year:
            # files_by_year[year] = {}
            files_by_year.setdefault(year, set())

        # Adiciona apenas SHAs que realmente existem na pasta
        for sha in sha_list:
            if os.path.exists(f"{json_folder_path}/{sha}.json"):
                files_by_year[year].add(sha)

    return files_by_year

def compres_jsons(files_by_year: dict, json_folder_path: Path):
    print("\nIniciando compactação por ano...")
    for year, shas in files_by_year.items():
        zip_filename = f"{OUTPUT_ZIPS_PATH}/cord19_jsons_{year}.zip"
        print(f"Criando {zip_filename} ({len(shas)} arquivos)...")

        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Usa tqdm para barra de progresso
            for sha in tqdm(shas, desc=f"Ano {year}"):
                json_file = f"{json_folder_path}/{sha}.json"
                # Adiciona ao zip mantendo a estrutura flat
                zipf.write(json_file, arcname=f"{sha}.json")


def main():
    dataset_path = Path(kagglehub.dataset_download(DATASET_HANDLE))

    metadata_path = dataset_path / "metadata.csv"
    json_folder_path = dataset_path / "document_parses" / "pdf_json"
    # assume que catalogo, schema e volume existem
    catalog_existe = assert_uc_components("catalog", CATALOG_NAME)
    schema_existe = assert_uc_components("schema", f"{CATALOG_NAME}.{SCHEMA_NAME}")
    volume_existe = assert_uc_components("volume", f"{CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_BRONZE}")

    if not catalog_existe:
        raise ValueError("Catalogo não existe... execute set_up.py")

    if not schema_existe:
        raise ValueError("Schema não existe... execute set_up.py")

    if not volume_existe:
        raise ValueError("Volume não existe... execute set_up.py")

    # 2. Upload dos arquivos ZIP
    print("Iniciando processamento de metadados...")

    # 1. Ler e filtrar arquivo de metadata
    metadata_df = metadata_csv_file(metadata_path)
    # 2. Mapear ano e valores de sha dos json
    # Estrutura: { 2020: ['sha1', 'sha2'], 2021: [...] }
    files_by_year = map_year_files(metadata_df, json_folder_path)
    # 3. Comprimir arquivos por ano
    compres_jsons(files_by_year, json_folder_path)

    existing_zips = list(OUTPUT_ZIPS_PATH.glob("*.zip"))
    del metadata_df
    del files_by_year


    print(f"\n--- Enviando para o Databricks ---")
    config = Config(
        host=os.environ.get("DATABRICKS_HOST"),
        token=os.environ.get("DATABRICKS_TOKEN"),
        auth_type='pat'
    )
    w = WorkspaceClient(config=config)
    dbfs_volume_path = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_BRONZE}/kaggle_downloads"


    print("\n--- Enviando Metadata ---")
    w.files.upload_from(
            f"{dbfs_volume_path}/{metadata_path.name}",
            metadata_path
        )
    print(f"Successfully uploaded {metadata_path} to {dbfs_volume_path}/{metadata_path.name}")


    print("\n--- Enviando JSONs comprimidos ---")
    for zip_file in existing_zips:
        remote_path = f"{dbfs_volume_path}/{zip_file.name}"
        file_size = zip_file.stat().st_size
        print(f"\nPreparando: {remote_path}")
        try:
            # Upload the file
            w.files.upload_from(
                remote_path,
                zip_file,
                overwrite=True # Set to True to overwrite if the file exists
            )
            print(f"Successfully uploaded {zip_file} to {remote_path}")
            os.remove(zip_file)
        except Exception as e:
            print(f"An error occurred: {e}")

    shutil.rmtree(dataset_path.parent)
    print("\nIngestão concluída com sucesso!")

if __name__ == "__main__":
    main()