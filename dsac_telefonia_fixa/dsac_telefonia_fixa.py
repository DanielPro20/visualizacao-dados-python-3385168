# =============================================================================
# DSAC - Dados de Assinatura, Habilitação e Troncos da Telefonia Fixa
# -----------------------------------------------------------------------------
# Python (sem Spark) usando AWS SDK for pandas (awswrangler).
#
#   Origem : Athena, database trusted_db
#            (dsac_net_fone, dsac_claro_fone, dsac_sumexec)
#   Destino: Athena, database curated_db, tabela dsac_p1_sub2
#            (Parquet no S3, particionado por ano)
#
# Como funciona:
#   1. A query SQL original roda no próprio Athena (o processamento pesado fica lá).
#   2. O resultado (poucas linhas) volta como DataFrame pandas.
#   3. É gravado em Parquet no S3 e registrado no Glue Catalog como
#      curated_db.dsac_p1_sub2. Reprocessar um ano substitui só aquele ano.
#
# Onde rodar: Jupyter (cole numa célula) ou como script (python dsac_telefonia_fixa.py).
#   pip install awswrangler
#   Credenciais AWS: aws configure / variáveis de ambiente / role da máquina.
#   Em AWS Glue Python Shell: job parameter --additional-python-modules awswrangler
#
# Permissões necessárias (usuário ou role que executa):
#   - athena:StartQueryExecution / GetQueryExecution / GetQueryResults
#   - glue:GetDatabase / GetTable / GetPartitions (trusted_db e curated_db)
#   - glue:CreateTable / UpdateTable / BatchCreatePartition (curated_db)
#   - s3:GetObject / ListBucket nos buckets do trusted_db
#   - s3:PutObject / DeleteObject / ListBucket no bucket do curated_db
#   - s3 no bucket de resultados do Athena
#   - Lake Formation (se houver): SELECT no trusted_db e CREATE_TABLE/ALTER no curated_db
# =============================================================================

import awswrangler as wr
import boto3

# -----------------------------------------------------------------------------
# PARÂMETROS (edite aqui)
# -----------------------------------------------------------------------------
ANO = "2025"                         # ano de referência

DATABASE_ORIGEM = "trusted_db"
DATABASE_DESTINO = "curated_db"
TABELA_DESTINO = "dsac_p1_sub2"

# Caminho S3 da tabela de destino. None = descobre sozinho:
#   - se a tabela já existe, usa o LOCATION dela;
#   - senão, usa o LOCATION do database curated_db + /dsac_p1_sub2/
S3_DESTINO = None                    # ex.: "s3://meu-bucket/curated/dsac_p1_sub2/"

WORKGROUP = None                     # workgroup do Athena (None = primary)
REGIAO = None                        # ex.: "sa-east-1" (None = região padrão das credenciais)

if not (len(ANO) == 4 and ANO.isdigit()):
    raise ValueError("ANO deve ter 4 dígitos, ex.: 2025")

sessao = boto3.Session(region_name=REGIAO)

# -----------------------------------------------------------------------------
# QUERY ORIGINAL (somente o ano foi parametrizado)
# -----------------------------------------------------------------------------
SQL = f"""
--BASE E INSTALAÇÕES NET FONE/CLARO FONE
select servico_anatel,produto_anatel,sum(total) total
from (
select ano,produto,indicador,
case when indicador='Base de Conectados' and tipo_produto='RESIDENCIAL' then 'NVTF.1.3.1.1'
     when indicador='Base de Conectados' and tipo_produto='EMPRESA' then 'NVTF.3.2.1.1'
     when indicador='Instalação' and tipo_produto='RESIDENCIAL' then 'NVTF.1.3.2.1'
     when indicador='Instalação' and tipo_produto='EMPRESA' then 'NVTF.3.2.2.1'
       else 'ND' end servico_anatel,
case when indicador='Base de Conectados' and tipo_produto='RESIDENCIAL' then 'Assinatura Residencial'
     when indicador='Base de Conectados' and tipo_produto='EMPRESA' then 'Assinatura Não Residencial'
     when indicador='Instalação' and tipo_produto='RESIDENCIAL' then 'Habilitação Residencial'
     when indicador='Instalação' and tipo_produto='EMPRESA' then 'Habilitação Não Residencial'
       else 'ND' end produto_anatel,
sum(total) total
from (
select substr(cast(a.data_base as varchar),1,4) ano,a.nm_tipo_produto_marca_bi produto,a.nm_indicador_negocio_bi indicador,
       case when a.nm_linha_negocio_bi like '%Empresa%' then 'EMPRESA'
         else 'RESIDENCIAL' end tipo_produto, sum(a.qtde) total
from {DATABASE_ORIGEM}.dsac_net_fone a
where cast(a.data_base as varchar) like '{ANO}%'
and a.nm_indicador_negocio_bi in ('Base de Conectados','Instalação')
group by substr(cast(a.data_base as varchar),1,4),a.nm_tipo_produto_marca_bi,a.nm_indicador_negocio_bi,a.nm_linha_negocio_bi
union all
select substr(cast(a.data_base as varchar),1,4) ano,a.nm_tipo_produto_marca_bi produto,a.nm_indicador_negocio_bi indicador,
       case when a.nm_linha_negocio_bi like '%Empresa%' then 'EMPRESA'
         else 'RESIDENCIAL' end tipo_produto, sum(a.qtde) total
from {DATABASE_ORIGEM}.dsac_claro_fone a
where cast(a.data_base as varchar) like '{ANO}%'
and a.nm_indicador_negocio_bi in ('Base de Conectados','Instalação')
group by substr(cast(a.data_base as varchar),1,4),a.nm_tipo_produto_marca_bi,a.nm_indicador_negocio_bi,a.nm_linha_negocio_bi
) t1
group by ano,produto,indicador,tipo_produto

union all

--BASE E INSTALAÇÕES CLARO FONE
select substr(cast(a.data_base as varchar),1,4) ano,a.nm_tipo_produto_marca_bi produto,a.nm_indicador_negocio_bi indicador,
       case when a.nm_indicador_negocio_bi='Base de Conectados' and a.nm_tipo_produto_marca_bi='Claro Fone' then 'NVTF.8.2.1.1'
            when a.nm_indicador_negocio_bi='Instalação' and a.nm_tipo_produto_marca_bi='Claro Fone' then 'NVTF.8.3.1.1'
              else 'ND' end servico_anatel,
       case when a.nm_indicador_negocio_bi='Base de Conectados' and a.nm_tipo_produto_marca_bi='Claro Fone' then 'PUCs Local'
            when a.nm_indicador_negocio_bi='Instalação' and a.nm_tipo_produto_marca_bi='Claro Fone' then 'Outras linhas de produto_Outros Serviços_Outros Serviços_Outros Serviços'
              else 'ND' end produto_anatel,
       sum(a.qtde) total
from {DATABASE_ORIGEM}.dsac_claro_fone a
where cast(a.data_base as varchar) like '{ANO}%'
and a.nm_indicador_negocio_bi in ('Base de Conectados','Instalação')
group by substr(cast(a.data_base as varchar),1,4),a.nm_tipo_produto_marca_bi,a.nm_indicador_negocio_bi

union all

--BASE E ATIVAÇÕES E1S EMPRESARIAL
select ano,produto,indicador,
case when indicador='BASE' then 'NVTF.2.2.1.1'
     when indicador='ATIVAÇÕES' then 'NVTF.2.2.2.1'
       else 'ND' end servico_anatel,
case when indicador='BASE' then 'Assinatura Tronco'
     when indicador='ATIVAÇÕES' then 'Habilitação Tronco'
       else 'ND' end produto_anatel,
sum(total) total
from (
select substr(cast(ano_mes as varchar),1,4) ano,categoria produto,'BASE' indicador, round(sum(qtd_base),0) total
from {DATABASE_ORIGEM}.dsac_sumexec
where categoria like '%E1%'
and cast(ano_mes as varchar) like '{ANO}%'
group by ano_mes,categoria
union all
select substr(cast(ano_mes as varchar),1,4) ano,categoria produto,'ATIVAÇÕES' indicador, round(sum(qtd_ativ),0) total
from {DATABASE_ORIGEM}.dsac_sumexec
where categoria like '%E1%'
and cast(ano_mes as varchar) like '{ANO}%'
group by ano_mes,categoria
) t2
group by ano,produto,indicador
) t3
group by servico_anatel,produto_anatel
order by servico_anatel
"""


# -----------------------------------------------------------------------------
# FUNÇÕES
# -----------------------------------------------------------------------------
def descobrir_s3_destino():
    """Define o caminho S3 da tabela de destino (ver comentário de S3_DESTINO)."""
    if S3_DESTINO:
        return S3_DESTINO.rstrip("/") + "/"

    # Tabela já existe: grava no mesmo LOCATION
    if wr.catalog.does_table_exist(database=DATABASE_DESTINO, table=TABELA_DESTINO,
                                   boto3_session=sessao):
        local = wr.catalog.get_table_location(database=DATABASE_DESTINO, table=TABELA_DESTINO,
                                              boto3_session=sessao)
        return local.rstrip("/") + "/"

    # Tabela nova: LOCATION do database curated_db + nome da tabela
    db = sessao.client("glue").get_database(Name=DATABASE_DESTINO)["Database"]
    local_db = db.get("LocationUri")
    if not local_db:
        raise ValueError(
            f"O database {DATABASE_DESTINO} não tem LOCATION definido no Glue. "
            "Preencha S3_DESTINO com o caminho S3 da tabela."
        )
    return local_db.rstrip("/") + f"/{TABELA_DESTINO}/"


# -----------------------------------------------------------------------------
# 1. EXECUTA A QUERY NO ATHENA (lendo do trusted_db)
# -----------------------------------------------------------------------------
print(f"Executando query no Athena ({DATABASE_ORIGEM}) para o ano {ANO}...")
df = wr.athena.read_sql_query(
    sql=SQL,
    database=DATABASE_ORIGEM,
    workgroup=WORKGROUP,
    ctas_approach=False,       # não cria tabelas temporárias no trusted_db
    boto3_session=sessao,
)

# -----------------------------------------------------------------------------
# 2. VALIDAÇÕES E AJUSTES
# -----------------------------------------------------------------------------
if df.empty:
    raise ValueError(f"Nenhum registro encontrado no {DATABASE_ORIGEM} para o ano {ANO}")

qtd_nd = int((df["servico_anatel"] == "ND").sum())
if qtd_nd:
    print(f"ATENÇÃO: {qtd_nd} linha(s) sem de/para Anatel (ND)")

df["total"] = df["total"].astype("float64")   # tipo estável entre execuções
df["ano"] = ANO                               # coluna de partição

print(f"{len(df)} linha(s) calculadas:")
print(df.to_string(index=False))

# -----------------------------------------------------------------------------
# 3. GRAVA NO ATHENA: curated_db.dsac_p1_sub2 (Parquet no S3, partição por ano)
#    overwrite_partitions = substitui só o ano processado; os outros anos ficam.
# -----------------------------------------------------------------------------
caminho = descobrir_s3_destino()
print(f"Gravando {DATABASE_DESTINO}.{TABELA_DESTINO} em {caminho} ...")

wr.s3.to_parquet(
    df=df,
    path=caminho,
    dataset=True,
    mode="overwrite_partitions",
    partition_cols=["ano"],
    database=DATABASE_DESTINO,
    table=TABELA_DESTINO,
    dtype={
        "servico_anatel": "string",
        "produto_anatel": "string",
        "total": "double",
        "ano": "string",
    },
    description="DSAC - Assinatura, Habilitação e Troncos da Telefonia Fixa (por serviço/produto Anatel)",
    columns_comments={
        "servico_anatel": "Código do serviço Anatel (NVTF)",
        "produto_anatel": "Produto Anatel",
        "total": "Quantidade total no ano",
        "ano": "Ano de referência (partição)",
    },
    boto3_session=sessao,
)

print(f"Concluído. Consulte no Athena: SELECT * FROM {DATABASE_DESTINO}.{TABELA_DESTINO} WHERE ano = '{ANO}'")
