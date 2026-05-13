import zipfile
import json
import pandas as pd
import os
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# Configuração de caminhos
# No Databricks, use o caminho do Volume. Localmente, ajuste para a sua pasta montada.
CATALOG_NAME = os.environ.get("CATALOG_NAME")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
VOLUME_BRONZE = os.environ.get("VOLUME_BRONZE")
OUTPUT_ARTIGOS_BR = os.environ.get("OUTPUT_ARTIGOS_BR")

BRONZE_VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_BRONZE}/"
OUTPUT_CSV_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_BRONZE}/{OUTPUT_ARTIGOS_BR}"


def identificar_artigos_br():
    shas_brasileiros = []
    
    # 2. Listar os ZIPs usando o dbutils (garante visibilidade no Databricks)
    try:
        arquivos_no_volume = dbutils.fs.ls(BRONZE_VOLUME_PATH)
        zips = [f.path.replace("dbfs:", "") for f in arquivos_no_volume if f.name.endswith(".zip")]
    except Exception as e:
        print(f"Erro ao listar volume: {e}")
        return

    print(f"📂 Encontrados {len(zips)} arquivos ZIP para varredura.")

    # 3. Loop sequencial (Seguro para o Driver da Free Edition)
    for zip_path in zips:
        nome_zip = os.path.basename(zip_path)
        print(f"--- Processando: {nome_zip} ---")
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                # Filtra apenas os JSONs
                jsons_internos = [f for f in z.namelist() if f.endswith('.json')]
                
                # Barra de progresso para cada ZIP
                for nome_json in tqdm(jsons_internos, desc=f"Lendo {nome_zip[:15]}"):
                    try:
                        with z.open(nome_json) as f:
                            data = json.load(f)
                            sha = data.get('paper_id')
                            
                            # Varre autores em busca de afiliação Brasil
                            metadata = data.get('metadata', {})
                            authors = metadata.get('authors', [])
                            
                            for auth in authors:
                                country = auth.get('affiliation', {}).get('location', {}).get('country', '')
                                if country and str(country).strip().lower() in ['brazil', 'brasil']:
                                    shas_brasileiros.append(sha)
                                    break # Achou um autor BR, pula para o próximo JSON
                    except:
                        continue # Ignora JSONs malformados
        except Exception as e:
            print(f"Erro ao abrir {nome_zip}: {e}")

    # 4. Consolidação e salvamento
    if shas_brasileiros:
        # Remove duplicatas caso o mesmo artigo esteja em zips diferentes
        df_br = pd.DataFrame(list(set(shas_brasileiros)), columns=["sha"])
        
        print(f"\n✅ Total de SHAs brasileiros identificados: {len(df_br)}")
        
        # Salva o CSV direto no Volume
        df_br.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8')
        print(f"💾 Arquivo salvo em: {OUTPUT_CSV_PATH}")
    else:
        print("⚠️ Nenhum artigo brasileiro foi identificado.")

if __name__ == "__main__":
    identificar_artigos_br()