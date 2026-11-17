%idle_timeout 60
%glue_version 5.0
%worker_type G.1X
%number_of_workers 2

# =============================================================================
# DSAC - Dados de Assinatura, Habilitação e Troncos da Telefonia Fixa
# -----------------------------------------------------------------------------
# Versão PySpark para AWS GLUE STUDIO NOTEBOOK.
# Como usar: cole este conteúdo inteiro em UMA célula do notebook e execute.
#
# Origem (Athena / Glue Data Catalog, database "trusted_db"):
#   trusted_db.dsac_net_fone | trusted_db.dsac_claro_fone | trusted_db.dsac_sumexec
#
# Blocos (iguais ao SQL original):
#   1. Base e Instalações NET FONE + CLARO FONE (Residencial x Empresa)
#   2. Base e Instalações CLARO FONE (PUCs Local / Outros Serviços)
#   3. Base e Ativações E1 Empresarial (arredondado por mês antes da soma do ano)
#   Obs.: como no SQL, dsac_claro_fone entra nos blocos 1 e 2.
#
# As linhas com % no topo configuram a sessão (só valem antes de ela iniciar).
#
# PRÉ-REQUISITO IAM (role Glue_Service) - sem isso a sessão não inicia
# ("not authorized to perform: iam:PassRole"). Política inline na role:
#   {"Version": "2012-10-17", "Statement": [{
#     "Effect": "Allow", "Action": "iam:PassRole",
#     "Resource": "arn:aws:iam::591667019086:role/Glue_Service",
#     "Condition": {"StringLike": {"iam:PassedToService": "glue.amazonaws.com"}}}]}
# A role também precisa ler o trusted_db (glue:GetDatabase/GetTable/GetPartitions,
# s3:GetObject/ListBucket nos buckets das tabelas e, se houver Lake Formation,
# SELECT nas 3 tabelas).
# =============================================================================

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# PARÂMETROS (edite aqui)
# -----------------------------------------------------------------------------
ANO = "2025"                 # ano de referência
DATABASE = "trusted_db"      # database (owner) das tabelas no Athena

SAIDA_S3 = None              # ex.: "s3://meu-bucket/refined/dsac_telefonia_fixa/"  (None = não grava)
SAIDA_TABELA = None          # ex.: "refined_db.dsac_telefonia_fixa"  (None = não registra no Athena)

# -----------------------------------------------------------------------------
# SESSÃO SPARK (reaproveita a sessão do Glue Studio)
# -----------------------------------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")  # overwrite só do ano gravado


# -----------------------------------------------------------------------------
# FUNÇÕES AUXILIARES
# -----------------------------------------------------------------------------
def ler_tabela(tabela):
    """Lê database.tabela do Glue Data Catalog, validando se a tabela existe."""
    if spark.sql(f"SHOW TABLES IN {DATABASE} LIKE '{tabela}'").count() == 0:
        raise ValueError(
            f"Tabela {DATABASE}.{tabela} não encontrada no Glue Data Catalog. "
            "Verifique --enable-glue-datacatalog e as permissões (IAM / Lake Formation)."
        )
    return spark.table(f"{DATABASE}.{tabela}")


def ano_de(coluna):
    """substr(cast(coluna as varchar), 1, 4)"""
    return F.substring(F.col(coluna).cast("string"), 1, 4)


def filtro_ano(coluna):
    """cast(coluna as varchar) like 'ANO%'"""
    return F.col(coluna).cast("string").startswith(ANO)


def case_when(regras):
    """CASE WHEN cond1 THEN v1 WHEN cond2 THEN v2 ... ELSE 'ND' END"""
    expr = F.when(regras[0][0], regras[0][1])
    for condicao, valor in regras[1:]:
        expr = expr.when(condicao, valor)
    return expr.otherwise("ND")


COLUNAS = ["ano", "produto", "indicador", "servico_anatel", "produto_anatel", "total"]
INDICADORES = ["Base de Conectados", "Instalação"]

# -----------------------------------------------------------------------------
# LEITURA DAS TABELAS (somente as colunas usadas)
# -----------------------------------------------------------------------------
colunas_fone = ["data_base", "nm_tipo_produto_marca_bi", "nm_indicador_negocio_bi",
                "nm_linha_negocio_bi", "qtde"]

df_net = ler_tabela("dsac_net_fone").select(*colunas_fone)
df_claro = ler_tabela("dsac_claro_fone").select(*colunas_fone)
df_sumexec = ler_tabela("dsac_sumexec").select("ano_mes", "categoria", "qtd_base", "qtd_ativ")


# -----------------------------------------------------------------------------
# BLOCO 1 - BASE E INSTALAÇÕES NET FONE / CLARO FONE
# -----------------------------------------------------------------------------
def agrega_fone(df):
    """Subquery t1: agrupa por ano, produto, indicador e linha de negócio."""
    return (
        df.where(filtro_ano("data_base"))
        .where(F.col("nm_indicador_negocio_bi").isin(INDICADORES))
        .groupBy(
            ano_de("data_base").alias("ano"),
            F.col("nm_tipo_produto_marca_bi").alias("produto"),
            F.col("nm_indicador_negocio_bi").alias("indicador"),
            "nm_linha_negocio_bi",
        )
        .agg(F.sum("qtde").alias("total"))
        # like '%Empresa%' -> EMPRESA ; demais (inclusive nulo) -> RESIDENCIAL
        .withColumn("tipo_produto",
                    F.when(F.col("nm_linha_negocio_bi").contains("Empresa"), "EMPRESA")
                    .otherwise("RESIDENCIAL"))
    )


t1 = agrega_fone(df_net).unionByName(agrega_fone(df_claro))

ind, tipo = F.col("indicador"), F.col("tipo_produto")
base_res = (ind == "Base de Conectados") & (tipo == "RESIDENCIAL")
base_emp = (ind == "Base de Conectados") & (tipo == "EMPRESA")
inst_res = (ind == "Instalação") & (tipo == "RESIDENCIAL")
inst_emp = (ind == "Instalação") & (tipo == "EMPRESA")

bloco1 = (
    t1.groupBy("ano", "produto", "indicador", "tipo_produto")
    .agg(F.sum("total").alias("total"))
    .withColumn("servico_anatel", case_when([
        (base_res, "NVTF.1.3.1.1"),
        (base_emp, "NVTF.3.2.1.1"),
        (inst_res, "NVTF.1.3.2.1"),
        (inst_emp, "NVTF.3.2.2.1"),
    ]))
    .withColumn("produto_anatel", case_when([
        (base_res, "Assinatura Residencial"),
        (base_emp, "Assinatura Não Residencial"),
        (inst_res, "Habilitação Residencial"),
        (inst_emp, "Habilitação Não Residencial"),
    ]))
    .select(*COLUNAS)
)

# -----------------------------------------------------------------------------
# BLOCO 2 - BASE E INSTALAÇÕES CLARO FONE
# -----------------------------------------------------------------------------
prod = F.col("produto")
base_claro = (ind == "Base de Conectados") & (prod == "Claro Fone")
inst_claro = (ind == "Instalação") & (prod == "Claro Fone")

bloco2 = (
    df_claro.where(filtro_ano("data_base"))
    .where(F.col("nm_indicador_negocio_bi").isin(INDICADORES))
    .groupBy(
        ano_de("data_base").alias("ano"),
        F.col("nm_tipo_produto_marca_bi").alias("produto"),
        F.col("nm_indicador_negocio_bi").alias("indicador"),
    )
    .agg(F.sum("qtde").alias("total"))
    .withColumn("servico_anatel", case_when([
        (base_claro, "NVTF.8.2.1.1"),
        (inst_claro, "NVTF.8.3.1.1"),
    ]))
    .withColumn("produto_anatel", case_when([
        (base_claro, "PUCs Local"),
        (inst_claro, "Outras linhas de produto_Outros Serviços_Outros Serviços_Outros Serviços"),
    ]))
    .select(*COLUNAS)
)

# -----------------------------------------------------------------------------
# BLOCO 3 - BASE E ATIVAÇÕES E1 EMPRESARIAL
# Soma e arredonda POR MÊS (group by ano_mes, categoria) e depois soma no ano.
# BASE e ATIVAÇÕES saem da mesma leitura e viram linhas com stack().
# -----------------------------------------------------------------------------
t2 = (
    df_sumexec.where(F.col("categoria").contains("E1"))      # like '%E1%'
    .where(filtro_ano("ano_mes"))
    .groupBy("ano_mes", "categoria")
    .agg(
        F.round(F.sum("qtd_base"), 0).alias("tot_base"),
        F.round(F.sum("qtd_ativ"), 0).alias("tot_ativ"),
    )
    .select(
        ano_de("ano_mes").alias("ano"),
        F.col("categoria").alias("produto"),
        F.expr("stack(2, 'BASE', tot_base, 'ATIVAÇÕES', tot_ativ) as (indicador, total)"),
    )
)

bloco3 = (
    t2.groupBy("ano", "produto", "indicador")
    .agg(F.sum("total").alias("total"))
    .withColumn("servico_anatel", case_when([
        (ind == "BASE", "NVTF.2.2.1.1"),
        (ind == "ATIVAÇÕES", "NVTF.2.2.2.1"),
    ]))
    .withColumn("produto_anatel", case_when([
        (ind == "BASE", "Assinatura Tronco"),
        (ind == "ATIVAÇÕES", "Habilitação Tronco"),
    ]))
    .select(*COLUNAS)
)

# -----------------------------------------------------------------------------
# RESULTADO FINAL (SELECT externo: soma por serviço/produto Anatel)
# -----------------------------------------------------------------------------
resultado = (
    bloco1.unionByName(bloco2).unionByName(bloco3)
    .groupBy("servico_anatel", "produto_anatel")
    .agg(F.sum("total").alias("total"))
    .orderBy("servico_anatel")
    .cache()
)

qtd = resultado.count()
if qtd == 0:
    raise ValueError(f"Nenhum registro encontrado para o ano {ANO}")

qtd_nd = resultado.where(F.col("servico_anatel") == "ND").count()
if qtd_nd:
    print(f"ATENÇÃO: {qtd_nd} linha(s) sem de/para Anatel (ND)")

print(f"DSAC telefonia fixa {ANO}: {qtd} linha(s)")
resultado.show(100, truncate=False)

# -----------------------------------------------------------------------------
# GRAVAÇÃO (opcional) - Parquet no S3 particionado por ano; com SAIDA_TABELA
# a tabela é registrada no Glue Catalog e fica consultável no Athena.
# -----------------------------------------------------------------------------
if SAIDA_S3:
    saida = resultado.withColumn("ano", F.lit(ANO)).coalesce(1)
    if SAIDA_TABELA:
        db_dest, tb_dest = SAIDA_TABELA.split(".")
        if spark.sql(f"SHOW TABLES IN {db_dest} LIKE '{tb_dest}'").count() > 0:
            # tabela já existe: substitui só a partição do ano processado
            saida.select(*spark.table(SAIDA_TABELA).columns).write.insertInto(SAIDA_TABELA, overwrite=True)
        else:
            (saida.write.mode("overwrite").format("parquet").partitionBy("ano")
             .option("path", SAIDA_S3).saveAsTable(SAIDA_TABELA))
        print(f"Gravado em {SAIDA_TABELA} ({SAIDA_S3})")
    else:
        saida.write.mode("overwrite").format("parquet").partitionBy("ano").save(SAIDA_S3)
        print(f"Gravado em {SAIDA_S3}")
