"""
Dados de Assinatura, Habilitação e Troncos da Telefonia Fixa - DSAC
====================================================================

Versão PySpark da query SQL que consolida, por serviço/produto Anatel:

    1. Base e Instalações NET FONE + CLARO FONE (Residencial x Empresa)
    2. Base e Instalações CLARO FONE (PUCs Local / Outros Serviços)
    3. Base e Ativações de troncos E1 empresariais (dsac_sumexec)

Tabelas de origem (AWS Athena, database/owner "trusted_db"):
    trusted_db.dsac_net_fone, trusted_db.dsac_claro_fone e trusted_db.dsac_sumexec.

Como o Spark lê as tabelas do Athena
------------------------------------
O Athena não armazena dados: as tabelas ficam registradas no AWS Glue Data Catalog
e os arquivos no S3. O Spark usa o MESMO Glue Data Catalog como metastore e lê os
arquivos direto do S3 (sem passar pelo motor do Athena, sem custo de scan do Athena).

    - AWS Glue Job (Spark): marque "Use Glue Data Catalog as the Hive metastore"
      (parâmetro --enable-glue-datacatalog).
    - Amazon EMR: classificação "spark-hive-site" com
      hive.metastore.client.factory.class =
      com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory
      (o script também define essa configuração na SparkSession).
    - A role IAM do job precisa de: glue:GetDatabase/GetTable/GetPartitions no
      trusted_db e s3:GetObject/ListBucket nos buckets das tabelas (e, se gravar,
      glue:CreateTable/UpdateTable + s3:PutObject/DeleteObject no destino).

A lógica foi mantida 1:1 com o SQL original, inclusive:
    - a tabela dsac_claro_fone é contabilizada nos blocos 1 E 2 (como no SQL);
    - no bloco 3 o arredondamento é feito por mês (ano_mes) antes da soma anual.

Execução: este arquivo único roda tanto via spark-submit / AWS Glue Job quanto
colado inteiro em UMA célula de notebook (o ano e os destinos podem ser alterados
nos defaults de parse_args).

AWS Glue Studio notebook (interactive session)
----------------------------------------------
    - Cole o arquivo inteiro numa célula e execute. A SparkSession da sessão Glue é
      reaproveitada (não é recriada nem encerrada).
    - Se aparecer "Tabela ... não encontrada", reinicie a sessão com a célula abaixo
      ANTES de qualquer código (magics só valem antes de a sessão iniciar):

          %%configure
          {"--enable-glue-datacatalog": "true"}

    - A role IAM da sessão precisa das permissões listadas acima e, se o trusted_db estiver
      sob AWS Lake Formation, de permissão SELECT/DESCRIBE nas 3 tabelas.

Exemplos:

    # Apenas exibe o resultado
    spark-submit dsac_telefonia_fixa.py --ano 2025

    # Grava no S3 e registra a tabela no Glue Catalog (consultável no Athena)
    spark-submit dsac_telefonia_fixa.py --ano 2025 \
        --saida-tabela refined_db.dsac_telefonia_fixa \
        --saida-s3 s3://meu-bucket/refined/dsac_telefonia_fixa/

Códigos de saída: 0 = sucesso | 1 = erro de execução | 2 = erro de validação dos dados.
"""


import argparse
import logging
import sys
from typing import Iterable, List, Optional, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("dsac_telefonia_fixa")

# ---------------------------------------------------------------------------
# Constantes de negócio (centralizadas para facilitar manutenção)
# ---------------------------------------------------------------------------
IND_BASE = "Base de Conectados"
IND_INSTALACAO = "Instalação"
INDICADORES_FONE = [IND_BASE, IND_INSTALACAO]

TIPO_RESIDENCIAL = "RESIDENCIAL"
TIPO_EMPRESA = "EMPRESA"

PRODUTO_CLARO_FONE = "Claro Fone"

IND_E1_BASE = "BASE"
IND_E1_ATIVACOES = "ATIVAÇÕES"

NAO_DEFINIDO = "ND"

# Colunas mínimas que cada tabela de origem precisa ter
COLUNAS_FONE = ["data_base", "nm_tipo_produto_marca_bi", "nm_indicador_negocio_bi",
                "nm_linha_negocio_bi", "qtde"]
COLUNAS_SUMEXEC = ["ano_mes", "categoria", "qtd_base", "qtd_ativ"]

# Layout final (equivalente ao SELECT externo)
COLUNAS_SAIDA = ["servico_anatel", "produto_anatel", "total"]
# Layout intermediário comum aos 3 blocos (equivalente às colunas do UNION ALL)
COLUNAS_UNIAO = ["ano", "produto", "indicador", "servico_anatel", "produto_anatel", "total"]


class ErroValidacao(Exception):
    """Erro de qualidade/estrutura dos dados de entrada."""


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------
def _ano_de(coluna: str) -> Column:
    """Equivalente a substr(cast(coluna as varchar), 1, 4)."""
    return F.substring(F.col(coluna).cast("string"), 1, 4)


def _filtro_ano(coluna: str, ano: str) -> Column:
    """Equivalente a cast(coluna as varchar) like '<ano>%'."""
    return F.col(coluna).cast("string").startswith(ano)


def _mapear(regras: Iterable[Tuple[Column, str]], padrao: str = NAO_DEFINIDO) -> Column:
    """Monta um CASE WHEN ... THEN ... ELSE padrao END a partir de uma lista de regras."""
    expr = None
    for condicao, valor in regras:
        expr = F.when(condicao, valor) if expr is None else expr.when(condicao, valor)
    return expr.otherwise(padrao)


def validar_colunas(df: DataFrame, obrigatorias: List[str], nome: str) -> None:
    """Falha cedo (antes de qualquer processamento) se faltar coluna na origem."""
    faltando = [c for c in obrigatorias if c not in df.columns]
    if faltando:
        raise ErroValidacao(f"Tabela '{nome}' sem as colunas obrigatórias: {faltando}")


# ---------------------------------------------------------------------------
# Bloco 1 - BASE E INSTALAÇÕES NET FONE / CLARO FONE
# ---------------------------------------------------------------------------
def _agregar_fone_por_linha(df: DataFrame, ano: str) -> DataFrame:
    """Subquery interna do bloco 1 (aplicada a dsac_net_fone e a dsac_claro_fone).

    Agrupa por ano, produto, indicador e linha de negócio, classificando a linha
    em EMPRESA (contém 'Empresa') ou RESIDENCIAL (demais casos, inclusive nulos).
    """
    return (
        df.where(_filtro_ano("data_base", ano))
        .where(F.col("nm_indicador_negocio_bi").isin(INDICADORES_FONE))
        .groupBy(
            _ano_de("data_base").alias("ano"),
            F.col("nm_tipo_produto_marca_bi").alias("produto"),
            F.col("nm_indicador_negocio_bi").alias("indicador"),
            F.col("nm_linha_negocio_bi"),
        )
        .agg(F.sum("qtde").alias("total"))
        .withColumn(
            "tipo_produto",
            # LIKE '%Empresa%' é case-sensitive, assim como contains()
            F.when(F.col("nm_linha_negocio_bi").contains("Empresa"), TIPO_EMPRESA)
            .otherwise(TIPO_RESIDENCIAL),
        )
        .drop("nm_linha_negocio_bi")
    )


def bloco_net_claro_fone(df_net: DataFrame, df_claro: DataFrame, ano: str) -> DataFrame:
    """Bloco 1: une NET FONE + CLARO FONE e aplica o de/para Residencial x Não Residencial."""
    t1 = _agregar_fone_por_linha(df_net, ano).unionByName(_agregar_fone_por_linha(df_claro, ano))

    ind, tipo = F.col("indicador"), F.col("tipo_produto")
    base_res = (ind == IND_BASE) & (tipo == TIPO_RESIDENCIAL)
    base_emp = (ind == IND_BASE) & (tipo == TIPO_EMPRESA)
    inst_res = (ind == IND_INSTALACAO) & (tipo == TIPO_RESIDENCIAL)
    inst_emp = (ind == IND_INSTALACAO) & (tipo == TIPO_EMPRESA)

    return (
        t1.groupBy("ano", "produto", "indicador", "tipo_produto")
        .agg(F.sum("total").alias("total"))
        .withColumn("servico_anatel", _mapear([
            (base_res, "NVTF.1.3.1.1"),
            (base_emp, "NVTF.3.2.1.1"),
            (inst_res, "NVTF.1.3.2.1"),
            (inst_emp, "NVTF.3.2.2.1"),
        ]))
        .withColumn("produto_anatel", _mapear([
            (base_res, "Assinatura Residencial"),
            (base_emp, "Assinatura Não Residencial"),
            (inst_res, "Habilitação Residencial"),
            (inst_emp, "Habilitação Não Residencial"),
        ]))
        .select(*COLUNAS_UNIAO)
    )


# ---------------------------------------------------------------------------
# Bloco 2 - BASE E INSTALAÇÕES CLARO FONE
# ---------------------------------------------------------------------------
def bloco_claro_fone(df_claro: DataFrame, ano: str) -> DataFrame:
    """Bloco 2: CLARO FONE agregado por produto/indicador (PUCs Local e Outros Serviços)."""
    ind, prod = F.col("indicador"), F.col("produto")
    base_claro = (ind == IND_BASE) & (prod == PRODUTO_CLARO_FONE)
    inst_claro = (ind == IND_INSTALACAO) & (prod == PRODUTO_CLARO_FONE)

    return (
        df_claro.where(_filtro_ano("data_base", ano))
        .where(F.col("nm_indicador_negocio_bi").isin(INDICADORES_FONE))
        .groupBy(
            _ano_de("data_base").alias("ano"),
            F.col("nm_tipo_produto_marca_bi").alias("produto"),
            F.col("nm_indicador_negocio_bi").alias("indicador"),
        )
        .agg(F.sum("qtde").alias("total"))
        .withColumn("servico_anatel", _mapear([
            (base_claro, "NVTF.8.2.1.1"),
            (inst_claro, "NVTF.8.3.1.1"),
        ]))
        .withColumn("produto_anatel", _mapear([
            (base_claro, "PUCs Local"),
            (inst_claro, "Outras linhas de produto_Outros Serviços_Outros Serviços_Outros Serviços"),
        ]))
        .select(*COLUNAS_UNIAO)
    )


# ---------------------------------------------------------------------------
# Bloco 3 - BASE E ATIVAÇÕES E1 EMPRESARIAL
# ---------------------------------------------------------------------------
def bloco_e1_empresarial(df_sumexec: DataFrame, ano: str) -> DataFrame:
    """Bloco 3: troncos E1 (categoria contém 'E1').

    Igual ao SQL: soma e arredonda POR MÊS (group by ano_mes, categoria) e só
    depois soma no ano. A base (qtd_base) e as ativações (qtd_ativ) são
    calculadas na mesma passada e depois "despivotadas" com stack(), evitando
    ler a tabela duas vezes como no UNION ALL original.
    """
    mensal = (
        df_sumexec.where(F.col("categoria").contains("E1"))
        .where(_filtro_ano("ano_mes", ano))
        .groupBy("ano_mes", "categoria")
        .agg(
            # F.round = HALF_UP (mesmo comportamento do ROUND do Presto/Trino/Hive)
            F.round(F.sum("qtd_base"), 0).alias("tot_base"),
            F.round(F.sum("qtd_ativ"), 0).alias("tot_ativ"),
        )
        .select(
            _ano_de("ano_mes").alias("ano"),
            F.col("categoria").alias("produto"),
            F.expr(f"stack(2, '{IND_E1_BASE}', tot_base, '{IND_E1_ATIVACOES}', tot_ativ) "
                   "as (indicador, total)"),
        )
    )

    ind = F.col("indicador")
    return (
        mensal.groupBy("ano", "produto", "indicador")
        .agg(F.sum("total").alias("total"))
        .withColumn("servico_anatel", _mapear([
            (ind == IND_E1_BASE, "NVTF.2.2.1.1"),
            (ind == IND_E1_ATIVACOES, "NVTF.2.2.2.1"),
        ]))
        .withColumn("produto_anatel", _mapear([
            (ind == IND_E1_BASE, "Assinatura Tronco"),
            (ind == IND_E1_ATIVACOES, "Habilitação Tronco"),
        ]))
        .select(*COLUNAS_UNIAO)
    )


# ---------------------------------------------------------------------------
# Consolidação final
# ---------------------------------------------------------------------------
def calcular_dsac(df_net: DataFrame, df_claro: DataFrame, df_sumexec: DataFrame,
                  ano: str) -> DataFrame:
    """Une os 3 blocos e totaliza por serviço/produto Anatel (SELECT externo)."""
    validar_colunas(df_net, COLUNAS_FONE, "dsac_net_fone")
    validar_colunas(df_claro, COLUNAS_FONE, "dsac_claro_fone")
    validar_colunas(df_sumexec, COLUNAS_SUMEXEC, "dsac_sumexec")

    # Lemos apenas as colunas necessárias (column pruning em fontes colunares)
    df_net = df_net.select(*COLUNAS_FONE)
    df_claro = df_claro.select(*COLUNAS_FONE)
    df_sumexec = df_sumexec.select(*COLUNAS_SUMEXEC)

    t3 = (
        bloco_net_claro_fone(df_net, df_claro, ano)
        .unionByName(bloco_claro_fone(df_claro, ano))
        .unionByName(bloco_e1_empresarial(df_sumexec, ano))
    )

    return (
        t3.groupBy("servico_anatel", "produto_anatel")
        .agg(F.sum("total").alias("total"))
        .orderBy("servico_anatel")
        .select(*COLUNAS_SAIDA)
    )


# ---------------------------------------------------------------------------
# Infraestrutura (Spark, leitura, escrita, CLI)
# ---------------------------------------------------------------------------
# Database (owner) das tabelas de origem no Athena / Glue Data Catalog
DATABASE_ORIGEM = "trusted_db"
TABELAS_ORIGEM = {
    "net_fone": "dsac_net_fone",
    "claro_fone": "dsac_claro_fone",
    "sumexec": "dsac_sumexec",
}


def criar_spark(app_name: str) -> SparkSession:
    """SparkSession usando o AWS Glue Data Catalog (o catálogo do Athena) como metastore.

    - hive.metastore.client.factory.class: faz o Spark enxergar os databases/tabelas
      do Athena (Glue). Em Glue Jobs isso já vem com --enable-glue-datacatalog; em
      EMR pode vir da classificação spark-hive-site. Definir aqui é inofensivo.
    - partitionOverwriteMode=dynamic: no overwrite de tabela particionada, substitui
      só as partições gravadas (não apaga o histórico de outros anos).
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.adaptive.enabled", "true")   # AQE ajusta partições de shuffle
        .config("spark.sql.session.timeZone", "UTC")     # evita diferenças entre ambientes
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
        .getOrCreate()
    )


def tabela_existe(spark: SparkSession, nome_completo: str) -> bool:
    """Verifica se database.tabela existe no catálogo.

    Usa SHOW TABLES em vez de spark.catalog.tableExists porque este último não
    existe no PySpark do Glue 3.0 (Spark 3.1).
    """
    database, tabela = nome_completo.lower().split(".", 1)
    return spark.sql(f"SHOW TABLES IN {database} LIKE '{tabela}'").limit(1).count() > 0


def ler_tabela_athena(spark: SparkSession, database: str, tabela: str) -> DataFrame:
    """Lê uma tabela do Athena (Glue Data Catalog), validando antes se ela existe.

    O nome é usado em minúsculas, pois o Athena/Glue armazena nomes em lowercase.
    """
    nome = f"{database}.{tabela}".lower()
    if not tabela_existe(spark, nome):
        raise ErroValidacao(
            f"Tabela '{nome}' não encontrada no Glue Data Catalog. Verifique se o job usa o "
            "Glue como metastore e se a role IAM tem permissão no database."
        )
    log.info("Lendo %s", nome)
    return spark.table(nome)


def gravar(df: DataFrame, tabela: Optional[str], caminho_s3: Optional[str], ano: str,
           modo: str) -> None:
    """Grava o resultado em Parquet no S3 e (opcionalmente) registra no Glue Catalog.

    - Parquet + tabela externa (com path no S3) = consultável diretamente no Athena.
    - A coluna 'ano' é adicionada e usada como partição, permitindo reprocessar um ano
      sem apagar os demais.
    - O resultado tem poucas linhas, por isso coalesce(1) gera um único arquivo.
    """
    df = df.withColumn("ano", F.lit(ano)).coalesce(1)
    escritor = df.write.mode(modo).format("parquet").partitionBy("ano")

    if tabela:
        if not caminho_s3:
            raise ErroValidacao("--saida-tabela exige --saida-s3 (tabela externa para o Athena)")
        log.info("Gravando tabela %s em %s (modo=%s)", tabela, caminho_s3, modo)
        spark = df.sparkSession
        if modo == "overwrite" and tabela_existe(spark, tabela):
            # Tabela já existe: insertInto respeita o overwrite dinâmico por partição
            # (só o ano processado é substituído). insertInto é posicional, então
            # alinhamos as colunas na ordem da tabela de destino.
            colunas_destino = spark.table(tabela).columns
            df.select(*colunas_destino).write.insertInto(tabela, overwrite=True)
        else:
            escritor.option("path", caminho_s3).saveAsTable(tabela)
    elif caminho_s3:
        log.info("Gravando parquet em %s (modo=%s)", caminho_s3, modo)
        escritor.save(caminho_s3)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DSAC - Assinatura, Habilitação e Troncos da Telefonia Fixa")
    p.add_argument("--ano", default="2025", help="Ano de referência (default: 2025)")
    p.add_argument("--database", default=DATABASE_ORIGEM,
                   help=f"Database (owner) das tabelas no Athena (default: {DATABASE_ORIGEM})")
    p.add_argument("--saida-tabela", default=None,
                   help="Tabela de destino no Glue/Athena (ex.: refined_db.dsac_telefonia_fixa)")
    p.add_argument("--saida-s3", default=None,
                   help="Caminho S3 de destino (ex.: s3://bucket/refined/dsac_telefonia_fixa/)")
    p.add_argument("--modo", default="overwrite", choices=["overwrite", "append", "errorifexists"])
    p.add_argument("--mostrar", action="store_true", help="Exibe o resultado no log (debug)")
    # parse_known_args: ignora argumentos extras que o AWS Glue injeta (--JOB_NAME etc.)
    args, _ = p.parse_known_args(argv)

    if not (len(args.ano) == 4 and args.ano.isdigit()):
        p.error("--ano deve ter 4 dígitos, ex.: 2025")
    return args


def configurar_log() -> None:
    """Log no stdout (saída da célula do notebook / CloudWatch do Glue Job).

    O Glue já configura o logging raiz, por isso o handler é anexado direto no
    logger do job (basicConfig seria ignorado). Evita handler duplicado ao
    reexecutar a célula.
    """
    log.setLevel(logging.INFO)
    log.propagate = False
    if not log.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
        log.addHandler(handler)


def main(argv: Optional[List[str]] = None) -> int:
    configurar_log()
    args = parse_args(argv)
    spark = criar_spark(f"dsac_telefonia_fixa_{args.ano}")

    try:
        log.info("Iniciando DSAC telefonia fixa - ano %s", args.ano)
        # Tabelas do Athena (database/owner trusted_db) via Glue Data Catalog
        df_net = ler_tabela_athena(spark, args.database, TABELAS_ORIGEM["net_fone"])
        df_claro = ler_tabela_athena(spark, args.database, TABELAS_ORIGEM["claro_fone"])
        df_sumexec = ler_tabela_athena(spark, args.database, TABELAS_ORIGEM["sumexec"])

        # cache: o resultado é pequeno e é usado para validação, log e escrita
        resultado = calcular_dsac(df_net, df_claro, df_sumexec, args.ano).cache()

        qtd = resultado.count()
        if qtd == 0:
            raise ErroValidacao(f"Nenhum registro encontrado para o ano {args.ano}")
        nd = resultado.where(F.col("servico_anatel") == NAO_DEFINIDO).count()
        if nd:
            # Mesma regra do SQL (cai em 'ND'), mas avisamos para não passar despercebido
            log.warning("%d linha(s) classificadas como 'ND' (sem de/para Anatel)", nd)
        log.info("Resultado com %d linha(s)", qtd)

        if args.mostrar or not (args.saida_tabela or args.saida_s3):
            resultado.show(truncate=False)

        if args.saida_tabela or args.saida_s3:
            gravar(resultado, args.saida_tabela, args.saida_s3, args.ano, args.modo)

        log.info("Processo finalizado com sucesso")
        return 0
    except ErroValidacao as exc:
        log.error("Erro de validação: %s", exc)
        return 2
    except Exception:  # noqa: BLE001 - registramos o stack trace completo
        log.exception("Falha na execução do job")
        return 1
    finally:
        # Em notebook (Jupyter/Glue Studio/EMR Studio) a sessão é compartilhada: não para
        if not EM_NOTEBOOK:
            spark.stop()


# Detecta se é JOB: o Glue Job sempre recebe --JOB_NAME e o spark-submit recebe o
# caminho deste arquivo em sys.argv[0]. Qualquer outro caso (Glue Studio notebook,
# Jupyter, EMR Studio) é tratado como notebook: nada de sys.exit nem spark.stop(),
# que derrubariam a sessão interativa.
_argv = sys.argv or [""]
EM_JOB = "--JOB_NAME" in _argv or _argv[0].endswith("dsac_telefonia_fixa.py")
EM_NOTEBOOK = not EM_JOB

if EM_JOB:
    # spark-submit / Glue Job: código de saída sinaliza sucesso ou falha ao orquestrador
    if __name__ == "__main__":
        sys.exit(main())
else:
    # Notebook: executa com os defaults de parse_args (edite-os para mudar ano/destino)
    codigo_saida = main([])
    print(f"Código de saída: {codigo_saida} (0 = sucesso | 1 = erro de execução | 2 = erro de validação)")
