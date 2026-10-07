from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Ajuste os caminhos e o nome da tabela
ORIGEM = "s3://bucket/raw/claro_notf_pcrf_sms/CLARO_NOTF_PCRF_SMS_*.CSV"
DESTINO = "s3://bucket/trusted/franquia_consumo/"
TABELA = "trusted_db.franquia_consumo"

spark = SparkSession.builder.appName("csv_para_tabela").enableHiveSupport().getOrCreate()

# Lê o CSV: separador "|", primeira linha é cabeçalho, tudo como texto
df = (
    spark.read
    .option("header", "true")
    .option("sep", "|")
    .option("encoding", "utf-8")
    .option("inferSchema", "false")
    .csv(ORIGEM)
)

# Partição: ano e mês tirados de DATA_EVENTO (yyyyMMddHHmmss)
df = (
    df.withColumn("ano_evento", F.substring("DATA_EVENTO", 1, 4))
      .withColumn("mes_evento", F.substring("DATA_EVENTO", 5, 2))
)

spark.sql("CREATE DATABASE IF NOT EXISTS trusted_db")

# Grava em parquet, particionado, e registra a tabela no catálogo
(
    df.write
    .mode("append")
    .format("parquet")
    .option("compression", "snappy")
    .partitionBy("ano_evento", "mes_evento")
    .option("path", DESTINO)
    .saveAsTable(TABELA)
)

spark.table(TABELA).groupBy("ano_evento", "mes_evento").count().show()
