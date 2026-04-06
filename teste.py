# Lendo o CSV de metadados
df_metadata = (spark.read
               .format("csv")
               .option("header", "true")
               .option("inferSchema", "true")
               .load("/Volumes/projeto_integrador_cors19/dados_cord19/bronze/metadata.csv"))

# Filtrando apenas o que importa para a POC (ex: artigos com abstract)
df_silver_meta = df_metadata.filter("abstract IS NOT NULL").select("sha", "title", "abstract", "publish_time", "authors")

# Salvando na Silver como Tabela Delta
df_silver_meta.write.mode("overwrite").saveAsTable("projeto_integrador_cors19.dados_cord19.teste")