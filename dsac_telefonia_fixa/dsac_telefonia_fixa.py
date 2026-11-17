# =============================================================================
# DSAC - Dados de Assinatura, Habilitação e Troncos da Telefonia Fixa
# -----------------------------------------------------------------------------
# Python puro com boto3 (já vem instalado no AWS Glue - não precisa de pip).
#
#   Origem : Athena, database trusted_db
#            (dsac_net_fone, dsac_claro_fone, dsac_sumexec)
#   Destino: Athena, database curated_db, tabela dsac_p1_sub2
#            (Parquet no S3, particionado por ano)
#
# Todo o processamento roda no próprio Athena:
#   1. CREATE EXTERNAL TABLE IF NOT EXISTS curated_db.dsac_p1_sub2
#   2. Remove o ano que será reprocessado (partição + arquivos no S3)
#   3. INSERT INTO curated_db.dsac_p1_sub2 SELECT ... (query original)
#   4. Exibe o resultado gravado
#
# Funciona em: Glue Python Shell job, Glue Spark job, Jupyter, script local.
#
# Permissões necessárias (role do job):
#   - athena:StartQueryExecution / GetQueryExecution / GetQueryResults
#   - glue:GetDatabase / GetTable / GetPartitions (trusted_db e curated_db)
#   - glue:CreateTable / BatchCreatePartition / BatchDeletePartition (curated_db)
#   - s3:GetObject / ListBucket nos buckets do trusted_db
#   - s3:PutObject / GetObject / DeleteObject / ListBucket no S3 do curated_db
#   - s3:PutObject / GetObject no bucket de resultados do Athena
#   - Lake Formation (se houver): SELECT no trusted_db; CREATE_TABLE/INSERT no curated_db
# =============================================================================

import time
from urllib.parse import urlparse

import boto3

# -----------------------------------------------------------------------------
# PARÂMETROS (edite aqui)
# -----------------------------------------------------------------------------
ANO = "2025"                         # ano de referência

DATABASE_ORIGEM = "trusted_db"
DATABASE_DESTINO = "curated_db"
TABELA_DESTINO = "dsac_p1_sub2"

# Caminho S3 da tabela de destino. None = usa o LOCATION da tabela (se já existir)
# ou o LOCATION do database curated_db + /dsac_p1_sub2/
S3_DESTINO = None                    # ex.: "s3://meu-bucket/curated/dsac_p1_sub2/"

# Onde o Athena grava o resultado das consultas. None = usa o configurado no workgroup.
S3_RESULTADOS_ATHENA = None          # ex.: "s3://meu-bucket/athena-results/"
WORKGROUP = "primary"
REGIAO = None                        # ex.: "sa-east-1" (None = região padrão do ambiente)

if not (len(ANO) == 4 and ANO.isdigit()):
    raise ValueError("ANO deve ter 4 dígitos, ex.: 2025")

sessao = boto3.Session(region_name=REGIAO)
athena = sessao.client("athena")
glue = sessao.client("glue")
s3 = sessao.client("s3")

TABELA = f"{DATABASE_DESTINO}.{TABELA_DESTINO}"

# -----------------------------------------------------------------------------
# QUERY ORIGINAL (somente o ano e o database foram parametrizados; o ORDER BY
# final foi retirado porque não tem efeito num INSERT)
# -----------------------------------------------------------------------------
SQL_DSAC = f"""
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
"""


# -----------------------------------------------------------------------------
# FUNÇÕES
# -----------------------------------------------------------------------------
def executar_athena(sql, database=DATABASE_ORIGEM):
    """Executa um comando no Athena e espera terminar. Retorna o QueryExecutionId."""
    params = {
        "QueryString": sql,
        "QueryExecutionContext": {"Database": database},
        "WorkGroup": WORKGROUP,
    }
    if S3_RESULTADOS_ATHENA:
        params["ResultConfiguration"] = {"OutputLocation": S3_RESULTADOS_ATHENA}

    query_id = athena.start_query_execution(**params)["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        estado = status["State"]
        if estado == "SUCCEEDED":
            return query_id
        if estado in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Athena {estado}: {status.get('StateChangeReason', '')}\nSQL:\n{sql[:500]}")
        time.sleep(2)


def ler_resultado(query_id):
    """Lê o resultado de um SELECT do Athena como lista de dicionários."""
    linhas, colunas = [], None
    for pagina in athena.get_paginator("get_query_results").paginate(QueryExecutionId=query_id):
        for row in pagina["ResultSet"]["Rows"]:
            valores = [c.get("VarCharValue") for c in row["Data"]]
            if colunas is None:          # 1ª linha = cabeçalho
                colunas = valores
                continue
            linhas.append(dict(zip(colunas, valores)))
    return linhas


def descobrir_s3_destino():
    """Caminho S3 da tabela: parâmetro > LOCATION da tabela > LOCATION do database."""
    if S3_DESTINO:
        return S3_DESTINO.rstrip("/") + "/"
    try:
        tabela = glue.get_table(DatabaseName=DATABASE_DESTINO, Name=TABELA_DESTINO)["Table"]
        return tabela["StorageDescriptor"]["Location"].rstrip("/") + "/"
    except glue.exceptions.EntityNotFoundException:
        pass
    local_db = glue.get_database(Name=DATABASE_DESTINO)["Database"].get("LocationUri")
    if not local_db:
        raise ValueError(
            f"O database {DATABASE_DESTINO} não tem LOCATION no Glue. "
            "Preencha S3_DESTINO com o caminho S3 da tabela."
        )
    return local_db.rstrip("/") + f"/{TABELA_DESTINO}/"


def apagar_prefixo_s3(caminho_s3):
    """Apaga todos os arquivos de um prefixo S3 (dados antigos do ano reprocessado)."""
    url = urlparse(caminho_s3)
    bucket, prefixo = url.netloc, url.path.lstrip("/")
    total = 0
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefixo):
        objetos = [{"Key": o["Key"]} for o in pagina.get("Contents", [])]
        if objetos:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objetos, "Quiet": True})
            total += len(objetos)
    return total


# -----------------------------------------------------------------------------
# 1. CRIA A TABELA DE DESTINO (se ainda não existir)
# -----------------------------------------------------------------------------
caminho = descobrir_s3_destino()
print(f"Destino: {TABELA} em {caminho}")

executar_athena(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {TABELA} (
    servico_anatel string COMMENT 'Código do serviço Anatel (NVTF)',
    produto_anatel string COMMENT 'Produto Anatel',
    total          double COMMENT 'Quantidade total no ano'
)
COMMENT 'DSAC - Assinatura, Habilitação e Troncos da Telefonia Fixa'
PARTITIONED BY (ano string)
STORED AS PARQUET
LOCATION '{caminho}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')
""", database=DATABASE_DESTINO)

# -----------------------------------------------------------------------------
# 2. REMOVE O ANO QUE SERÁ REPROCESSADO (evita duplicar ao rodar de novo)
# -----------------------------------------------------------------------------
executar_athena(f"ALTER TABLE {TABELA} DROP IF EXISTS PARTITION (ano = '{ANO}')",
                database=DATABASE_DESTINO)
apagados = apagar_prefixo_s3(f"{caminho}ano={ANO}/")
print(f"Ano {ANO} limpo no destino ({apagados} arquivo(s) antigo(s) removido(s))")

# -----------------------------------------------------------------------------
# 3. CALCULA E GRAVA (a query roda no Athena lendo o trusted_db)
# -----------------------------------------------------------------------------
print(f"Executando DSAC {ANO} e gravando em {TABELA} ...")
executar_athena(f"""
INSERT INTO {TABELA}
SELECT servico_anatel, produto_anatel, cast(total as double) total, '{ANO}' ano
FROM ( {SQL_DSAC} ) dsac
""")

# -----------------------------------------------------------------------------
# 4. CONFERE O RESULTADO GRAVADO
# -----------------------------------------------------------------------------
resultado = ler_resultado(executar_athena(
    f"SELECT servico_anatel, produto_anatel, total FROM {TABELA} "
    f"WHERE ano = '{ANO}' ORDER BY servico_anatel",
    database=DATABASE_DESTINO,
))

if not resultado:
    raise ValueError(f"Nenhum registro gravado para o ano {ANO} (verifique os dados do {DATABASE_ORIGEM})")

qtd_nd = sum(1 for r in resultado if r["servico_anatel"] == "ND")
if qtd_nd:
    print(f"ATENÇÃO: {qtd_nd} linha(s) sem de/para Anatel (ND)")

print(f"\n{len(resultado)} linha(s) gravadas em {TABELA} (ano={ANO}):")
for r in resultado:
    print(f"  {r['servico_anatel']:<14} {r['produto_anatel']:<75} {r['total']}")

print(f"\nConsulte no Athena: SELECT * FROM {TABELA} WHERE ano = '{ANO}'")
