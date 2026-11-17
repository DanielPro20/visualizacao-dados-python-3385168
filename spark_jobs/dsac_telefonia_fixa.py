# -*- coding: utf-8 -*-
"""
Dados de Assinatura, Habilitação e Troncos da Telefonia Fixa - DSAC
Tabelas de origem: dsac_net_fone, dsac_claro_fone e dsac_sumexec

Conversão para PySpark da query SQL original. A lógica foi mantida 1:1:
  Bloco 1 - Base e Instalações NET FONE + CLARO FONE (Residencial x Empresa)
  Bloco 2 - Base e Instalações CLARO FONE (códigos NVTF.8.*)
  Bloco 3 - Base e Ativações de E1s Empresarial (troncos)
Resultado final: total por servico_anatel / produto_anatel, ordenado por servico_anatel.

Execução (exemplo):
    spark-submit dsac_telefonia_fixa.py --ano 2025
    spark-submit dsac_telefonia_fixa.py --ano 2025 --tabela-saida db.dsac_resultado_2025
"""

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
# Nomes das tabelas de origem. Se estiverem em um database/schema específico
# no ambiente produtivo, ajuste aqui (ex.: "dsac.dsac_net_fone").
TB_NET_FONE = "dsac_net_fone"
TB_CLARO_FONE = "dsac_claro_fone"
TB_SUMEXEC = "dsac_sumexec"

# Indicadores considerados nos blocos de NET FONE / CLARO FONE
INDICADORES = ["Base de Conectados", "Instalação"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("dsac_telefonia_fixa")


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------
def col_ano(nome_coluna: str):
    """Equivalente a: substr(cast(<coluna> as varchar), 1, 4)."""
    return F.substring(F.col(nome_coluna).cast("string"), 1, 4)


def filtro_ano(nome_coluna: str, ano: str):
    """Equivalente a: cast(<coluna> as varchar) like '<ano>%'."""
    return F.col(nome_coluna).cast("string").startswith(ano)


def le_fone(spark: SparkSession, tabela: str, ano: str) -> DataFrame:
    """
    Lê uma tabela de fone (NET/Claro) já filtrada pelo ano e pelos
    indicadores de interesse, selecionando só as colunas necessárias.
    """
    return (
        spark.table(tabela)
        .where(filtro_ano("data_base", ano))
        .where(F.col("nm_indicador_negocio_bi").isin(INDICADORES))
        .select(
            col_ano("data_base").alias("ano"),
            F.col("nm_tipo_produto_marca_bi").alias("produto"),
            F.col("nm_indicador_negocio_bi").alias("indicador"),
            F.col("nm_linha_negocio_bi").alias("linha_negocio"),
            F.col("qtde"),
        )
    )


# ---------------------------------------------------------------------------
# Bloco 1 - BASE E INSTALAÇÕES NET FONE / CLARO FONE
# ---------------------------------------------------------------------------
def bloco_net_claro_fone(df_net: DataFrame, df_claro: DataFrame) -> DataFrame:
    # Classifica a linha de negócio: se contém "Empresa" -> EMPRESA,
    # caso contrário (inclusive nulo) -> RESIDENCIAL.
    # Obs.: o LIKE do SQL é case-sensitive, assim como o .contains() abaixo.
    tipo_produto = (
        F.when(F.col("linha_negocio").contains("Empresa"), F.lit("EMPRESA"))
        .otherwise(F.lit("RESIDENCIAL"))
    )

    # Subquery t1: agregação de cada tabela e UNION ALL entre elas
    def agrega(df: DataFrame) -> DataFrame:
        return (
            df.groupBy("ano", "produto", "indicador", "linha_negocio")
            .agg(F.sum("qtde").alias("total"))
            .withColumn("tipo_produto", tipo_produto)
            .drop("linha_negocio")
        )

    t1 = agrega(df_net).unionByName(agrega(df_claro))

    # De-para indicador x tipo_produto -> código e descrição ANATEL
    is_base = F.col("indicador") == "Base de Conectados"
    is_inst = F.col("indicador") == "Instalação"
    is_res = F.col("tipo_produto") == "RESIDENCIAL"
    is_emp = F.col("tipo_produto") == "EMPRESA"

    servico_anatel = (
        F.when(is_base & is_res, "NVTF.1.3.1.1")
        .when(is_base & is_emp, "NVTF.3.2.1.1")
        .when(is_inst & is_res, "NVTF.1.3.2.1")
        .when(is_inst & is_emp, "NVTF.3.2.2.1")
        .otherwise("ND")
    )
    produto_anatel = (
        F.when(is_base & is_res, "Assinatura Residencial")
        .when(is_base & is_emp, "Assinatura Não Residencial")
        .when(is_inst & is_res, "Habilitação Residencial")
        .when(is_inst & is_emp, "Habilitação Não Residencial")
        .otherwise("ND")
    )

    # Agrupamento por ano, produto, indicador, tipo_produto (como no SQL)
    return (
        t1.groupBy("ano", "produto", "indicador", "tipo_produto")
        .agg(F.sum("total").alias("total"))
        .select(
            "ano",
            "produto",
            "indicador",
            servico_anatel.alias("servico_anatel"),
            produto_anatel.alias("produto_anatel"),
            "total",
        )
    )


# ---------------------------------------------------------------------------
# Bloco 2 - BASE E INSTALAÇÕES CLARO FONE (códigos NVTF.8.*)
# ---------------------------------------------------------------------------
def bloco_claro_fone(df_claro: DataFrame) -> DataFrame:
    # ATENÇÃO: a mesma base dsac_claro_fone também entra no Bloco 1.
    # Isso replica o comportamento do SQL original (a quantidade da Claro Fone
    # é reportada tanto em NVTF.1.3.* / NVTF.3.2.* quanto em NVTF.8.*).
    is_claro = F.col("produto") == "Claro Fone"
    is_base = F.col("indicador") == "Base de Conectados"
    is_inst = F.col("indicador") == "Instalação"

    servico_anatel = (
        F.when(is_base & is_claro, "NVTF.8.2.1.1")
        .when(is_inst & is_claro, "NVTF.8.3.1.1")
        .otherwise("ND")
    )
    produto_anatel = (
        F.when(is_base & is_claro, "PUCs Local")
        .when(
            is_inst & is_claro,
            "Outras linhas de produto_Outros Serviços_Outros Serviços_Outros Serviços",
        )
        .otherwise("ND")
    )

    return (
        df_claro.groupBy("ano", "produto", "indicador")
        .agg(F.sum("qtde").alias("total"))
        .select(
            "ano",
            "produto",
            "indicador",
            servico_anatel.alias("servico_anatel"),
            produto_anatel.alias("produto_anatel"),
            "total",
        )
    )


# ---------------------------------------------------------------------------
# Bloco 3 - BASE E ATIVAÇÕES E1s EMPRESARIAL (troncos)
# ---------------------------------------------------------------------------
def bloco_e1(spark: SparkSession, ano: str) -> DataFrame:
    # Leitura única da sumexec, filtrando categorias E1 e o ano
    base = (
        spark.table(TB_SUMEXEC)
        .where(F.col("categoria").contains("E1"))
        .where(filtro_ano("ano_mes", ano))
    )

    # No SQL o ROUND é aplicado por ano_mes + categoria (mensal) e só depois
    # somado no ano. Mantemos a mesma ordem para não alterar o arredondamento.
    mensal = base.groupBy("ano_mes", "categoria").agg(
        F.round(F.sum("qtd_base"), 0).alias("total_base"),
        F.round(F.sum("qtd_ativ"), 0).alias("total_ativ"),
    )

    # Subquery t2: UNION ALL de BASE e ATIVAÇÕES
    def monta(indicador: str, coluna: str) -> DataFrame:
        return mensal.select(
            col_ano("ano_mes").alias("ano"),
            F.col("categoria").alias("produto"),
            F.lit(indicador).alias("indicador"),
            F.col(coluna).alias("total"),
        )

    t2 = monta("BASE", "total_base").unionByName(monta("ATIVAÇÕES", "total_ativ"))

    servico_anatel = (
        F.when(F.col("indicador") == "BASE", "NVTF.2.2.1.1")
        .when(F.col("indicador") == "ATIVAÇÕES", "NVTF.2.2.2.1")
        .otherwise("ND")
    )
    produto_anatel = (
        F.when(F.col("indicador") == "BASE", "Assinatura Tronco")
        .when(F.col("indicador") == "ATIVAÇÕES", "Habilitação Tronco")
        .otherwise("ND")
    )

    return (
        t2.groupBy("ano", "produto", "indicador")
        .agg(F.sum("total").alias("total"))
        .select(
            "ano",
            "produto",
            "indicador",
            servico_anatel.alias("servico_anatel"),
            produto_anatel.alias("produto_anatel"),
            "total",
        )
    )


# ---------------------------------------------------------------------------
# Montagem do resultado final (t3)
# ---------------------------------------------------------------------------
def gera_resultado(spark: SparkSession, ano: str) -> DataFrame:
    # Leituras das bases de fone (feitas uma única vez cada).
    df_net = le_fone(spark, TB_NET_FONE, ano)
    # A Claro Fone é usada em dois blocos: cache evita ler a tabela 2x.
    df_claro = le_fone(spark, TB_CLARO_FONE, ano).cache()

    t3 = (
        bloco_net_claro_fone(df_net, df_claro)
        .unionByName(bloco_claro_fone(df_claro))
        .unionByName(bloco_e1(spark, ano))
    )

    # SELECT servico_anatel, produto_anatel, SUM(total) ... ORDER BY servico_anatel
    return (
        t3.groupBy("servico_anatel", "produto_anatel")
        .agg(F.sum("total").alias("total"))
        .orderBy("servico_anatel")
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def parse_args(argv):
    p = argparse.ArgumentParser(description="DSAC - Telefonia Fixa (Assinatura/Habilitação/Troncos)")
    p.add_argument("--ano", default="2025", help="Ano de referência (default: 2025)")
    p.add_argument(
        "--tabela-saida",
        default=None,
        help="Tabela destino (ex.: db.dsac_telefonia_fixa). Se omitido, apenas exibe o resultado.",
    )
    p.add_argument(
        "--modo-escrita",
        default="overwrite",
        choices=["overwrite", "append"],
        help="Modo de gravação na tabela destino (default: overwrite)",
    )
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])

    spark = (
        SparkSession.builder.appName(f"dsac_telefonia_fixa_{args.ano}")
        .enableHiveSupport()  # necessário para ler tabelas do metastore
        .getOrCreate()
    )

    try:
        log.info("Gerando DSAC Telefonia Fixa para o ano %s", args.ano)
        resultado = gera_resultado(spark, args.ano)

        if args.tabela_saida:
            # Inclui o ano de referência para rastreabilidade no destino
            (
                resultado.withColumn("ano_referencia", F.lit(args.ano))
                .write.mode(args.modo_escrita)
                .saveAsTable(args.tabela_saida)
            )
            log.info("Resultado gravado em %s (%s)", args.tabela_saida, args.modo_escrita)
        else:
            resultado.show(truncate=False)

    except Exception:
        log.exception("Falha na execução do DSAC Telefonia Fixa")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
