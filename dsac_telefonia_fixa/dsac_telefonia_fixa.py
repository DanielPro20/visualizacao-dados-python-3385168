"""
Dados de Assinatura, Habilitação e Troncos da Telefonia Fixa - DSAC
====================================================================

Versão PySpark da query SQL que consolida, por serviço/produto Anatel:

    1. Base e Instalações NET FONE + CLARO FONE (Residencial x Empresa)
    2. Base e Instalações CLARO FONE (PUCs Local / Outros Serviços)
    3. Base e Ativações de troncos E1 empresariais (dsac_sumexec)

Tabelas de origem: dsac_net_fone, dsac_claro_fone e dsac_sumexec.

A lógica foi mantida 1:1 com o SQL original, inclusive:
    - a tabela dsac_claro_fone é contabilizada nos blocos 1 E 2 (como no SQL);
    - no bloco 3 o arredondamento é feito por mês (ano_mes) antes da soma anual.

Execução (exemplos):

    spark-submit dsac_telefonia_fixa.py --ano 2025 \
        --database trusted_db \
        --saida-tabela meu_schema.dsac_telefonia_fixa_2025

    spark-submit dsac_telefonia_fixa.py --ano 2025 \
        --saida-path s3a://bucket/dsac/telefonia_fixa --formato parquet

Códigos de saída: 0 = sucesso | 1 = erro de execução | 2 = erro de validação dos dados.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Iterable, Optional

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


def _mapear(regras: Iterable[tuple[Column, str]], padrao: str = NAO_DEFINIDO) -> Column:
    """Monta um CASE WHEN ... THEN ... ELSE padrao END a partir de uma lista de regras."""
    expr = None
    for condicao, valor in regras:
        expr = F.when(condicao, valor) if expr is None else expr.when(condicao, valor)
    return expr.otherwise(padrao)


def validar_colunas(df: DataFrame, obrigatorias: list[str], nome: str) -> None:
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
def criar_spark(app_name: str) -> SparkSession:
    """SparkSession com Hive habilitado (para ler/gravar tabelas do metastore)."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")   # AQE ajusta partições de shuffle
        .config("spark.sql.session.timeZone", "UTC")     # evita diferenças entre ambientes
        .enableHiveSupport()
        .getOrCreate()
    )


def _nome_tabela(database: Optional[str], tabela: str) -> str:
    return f"{database}.{tabela}" if database else tabela


def gravar(df: DataFrame, tabela: Optional[str], path: Optional[str], formato: str,
           modo: str) -> None:
    """Grava o resultado em tabela do metastore e/ou em arquivos.

    O resultado tem poucas linhas, por isso coalesce(1) gera um único arquivo.
    """
    df = df.coalesce(1)
    if tabela:
        log.info("Gravando tabela %s (modo=%s)", tabela, modo)
        df.write.mode(modo).format("parquet").saveAsTable(tabela)
    if path:
        log.info("Gravando %s em %s (modo=%s)", formato, path, modo)
        escritor = df.write.mode(modo).format(formato)
        if formato == "csv":
            escritor = escritor.option("header", "true").option("encoding", "UTF-8")
        escritor.save(path)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DSAC - Assinatura, Habilitação e Troncos da Telefonia Fixa")
    p.add_argument("--ano", default="2025", help="Ano de referência (default: 2025)")
    p.add_argument("--database", default="trusted_db", help="Database de origem (camada trusted)")
    p.add_argument("--tabela-net-fone", default="dsac_net_fone")
    p.add_argument("--tabela-claro-fone", default="dsac_claro_fone")
    p.add_argument("--tabela-sumexec", default="dsac_sumexec")
    p.add_argument("--saida-tabela", default=None, help="Tabela de destino (ex.: schema.tabela)")
    p.add_argument("--saida-path", default=None, help="Diretório de destino (s3a://, hdfs://, local...)")
    p.add_argument("--formato", default="parquet", choices=["parquet", "csv", "delta", "orc"])
    p.add_argument("--modo", default="overwrite", choices=["overwrite", "append", "errorifexists"])
    p.add_argument("--mostrar", action="store_true", help="Exibe o resultado no log (debug)")
    args = p.parse_args(argv)

    if not (len(args.ano) == 4 and args.ano.isdigit()):
        p.error("--ano deve ter 4 dígitos, ex.: 2025")
    return args


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    args = parse_args(argv)
    spark = criar_spark(f"dsac_telefonia_fixa_{args.ano}")

    try:
        log.info("Iniciando DSAC telefonia fixa - ano %s", args.ano)
        df_net = spark.table(_nome_tabela(args.database, args.tabela_net_fone))
        df_claro = spark.table(_nome_tabela(args.database, args.tabela_claro_fone))
        df_sumexec = spark.table(_nome_tabela(args.database, args.tabela_sumexec))

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

        if args.mostrar or not (args.saida_tabela or args.saida_path):
            resultado.show(truncate=False)

        if args.saida_tabela or args.saida_path:
            gravar(resultado, args.saida_tabela, args.saida_path, args.formato, args.modo)

        log.info("Processo finalizado com sucesso")
        return 0
    except ErroValidacao as exc:
        log.error("Erro de validação: %s", exc)
        return 2
    except Exception:  # noqa: BLE001 - registramos o stack trace completo
        log.exception("Falha na execução do job")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
