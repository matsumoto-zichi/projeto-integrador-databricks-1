import zipfile
import os
from dotenv import load_dotenv
from pyspark.sql.functions import col, split, explode, asc, trim, substring, length

load_dotenv()

# envs
CATALOG_NAME = os.environ.get("CATALOG_NAME")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
BRONZE = os.environ.get('VOLUME_BRONZE')
SILVER = os.environ.get('VOLUME_SILVER')
SILVER_JSON_FOLDER = os.environ.get("SILVER_JSON_FOLDER")
ENRICHED_METADATA_CSV = os.environ.get("ENRICHED_METADATA_CSV")

# Caminhos
BRONZE_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{BRONZE}/"
SILVER_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{SILVER}/{SILVER_JSON_FOLDER}"
ENRICHED_METADATA_CSV_PATH = f"Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{BRONZE}/{ENRICHED_METADATA_CSV}"



def initial_df_ajusts(df):
    """
    Tratando SHAs separados por vírgula e filtrando apenas acima de 2020
    """

    # Split quebra a string em lista e Explode transforma cada item da lista em uma nova linha
    df_normalized = df.withColumn("sha_individual", explode(split(col("sha"), ",")))
    
    # Limpando espaços em branco que podem vir após a vírgula
    df_normalized = df_normalized.withColumn("sha_individual", trim(col("sha_individual")))
    
    # Filtrando apenas 2020 em diante
    df_normalized = df_normalized.filter(substring(col("publish_time"), 1, 4).cast("int") >= 2020)
    
    # Filtrando apenas artigos com abstract
    df_normalized = df_normalized.withColumn("length_abstract", length(col("abstract")))
    # df_normalized = df_normalized.filter("abstract IS NOT NULL OR length_abstract > 0")

    return df_normalized

def filter_br_articles(df):
    return df.filter(col("paper_br") == "1")

def filter_jif_rank(df):
    return (
        df
            .filter(col("paper_br") == "0")
            .filter(col("Rank").isNotNull())
            .orderBy(asc("Rank"))
            .limit(10000)
            )

def extract_selected_jsons(zip_filename, target_ids, output_path):
    zip_path = os.path.join(BRONZE_PATH, zip_filename)
    extracted_count = 0
    
    # Criar diretório de saída se não existir
    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)

    print(f"--- Processando: {zip_filename} ---")
    
    with zipfile.ZipFile(zip_path, 'r') as z:
        # Pegamos a lista de arquivos dentro do zip
        for file_info in z.infolist():
            # Extraímos o nome do arquivo ignorando pastas internas do zip
            # Ex: 'documentos/sha123.json' vira 'sha123'
            file_basename = os.path.basename(file_info.filename)
            file_id = file_basename.replace(".json", "")
            
            if file_id in target_ids:
                # Para evitar criar subpastas desnecessárias do zip na Silver, 
                # podemos ler e gravar o arquivo diretamente na raiz do output_path
                source = z.open(file_info)
                target_file_path = os.path.join(output_path, file_basename)
                
                with open(target_file_path, "wb") as target:
                    target.write(source.read())
                
                extracted_count += 1
                
                # Log a cada 500 arquivos para não travar a tela
                if extracted_count % 500 == 0:
                    print(f"Parcial: {extracted_count} arquivos extraídos de {zip_filename}...")
                
    return extracted_count


def main():
    # 1. Leitura com inferência de esquema
    df_raw = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(ENRICHED_METADATA_CSV_PATH)
    )
    
    # --- CORREÇÃO 1: Chamar os ajustes iniciais ---
    # Isso trata os SHAs múltiplos e filtra datas >= 2020
    df_metadata = initial_df_ajusts(df_raw)
    
    # --- CORREÇÃO 2: Filtrar SHAs nulos ANTES da união ---
    # Se o SHA é nulo, não conseguiremos buscar o JSON na Bronze, 
    # então eles não devem contar para a Silver.
    df_metadata = df_metadata.filter(col("sha_individual").isNotNull() & (col("sha_individual") != ""))

    # Selecionando Papers Brasileiros
    df_br = filter_br_articles(df_metadata)
    print(f"Artigos BRs processados: {df_br.count()}")

    # Selecionando Top 10.000 Journals
    df_top_journals = filter_jif_rank(df_metadata)
    print(f"Artigos Rank processados: {df_top_journals.count()}")

    # --- CORREÇÃO 3: Unir e remover duplicatas pela chave correta ---
    # Usamos unionByName e removemos duplicatas pelo sha_individual (o ID do arquivo)
    df_final_selection = df_br.unionByName(df_top_journals).dropDuplicates(["sha_individual"])

    # Set de IDs para busca rápida (usando o campo individualizado)
    ids_to_extract = {row.sha_individual for row in df_final_selection.select("sha_individual").collect()}

    print(f"Total de artigos únicos selecionados para extração: {len(ids_to_extract)}")


    # 2. Lista dos ZIPs
    zips = [
        "cord19_jsons_2020.zip", 
        "cord19_jsons_2021.zip", 
        "cord19_jsons_2022.zip", 
        "cord19_jsons_2024.zip"
    ]

    # Extração por arquivo zip
    total_geral = 0
    for z_file in zips:
        try:
            qtd = extract_selected_jsons(z_file, ids_to_extract, SILVER_PATH)
            total_geral += qtd
            print(f"Sucesso: {z_file} finalizado. Total extraído: {qtd}")
        except Exception as e:
            print(f"Erro ao processar o arquivo {z_file}: {e}")

    print(f"\nPipeline Finalizada! Total de JSONs na Silver: {total_geral}")

    arquivos_extraidos = set([f.replace(".json", "") for f in os.listdir(SILVER_PATH)])

    # 3. artigos não enviados
    ids_faltantes = ids_to_extract - arquivos_extraidos

    print(f"Total de IDs listados em metadado, mas não encontrados na relação de JSON: {len(ids_faltantes)}")

    
    display(df_metadata.filter(col("sha").contains(list(ids_faltantes)[0]) if len(ids_faltantes) > 0 else col("sha").isNull()))
    print("Salvando JSONs não encontrados na tabela projeto_integrador_cors19.dados_cord19.jsons_not_founded")
    
    # Renomear colunas para remover espaços e caracteres inválidos
    df_to_save = df_metadata.filter(col("sha").isin(list(ids_faltantes)) if len(ids_faltantes) > 0 else col("sha").isNull())
    for column in df_to_save.columns:
        df_to_save = df_to_save.withColumnRenamed(column, column.replace(" ", "_"))
    
    df_to_save.write.mode("overwrite").saveAsTable("projeto_integrador_cors19.dados_cord19.jsons_not_founded")

if __name__ == "__main__":
    main()
