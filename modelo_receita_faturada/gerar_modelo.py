"""Gera o arquivo modelo de execução (SQL Oracle) a partir do mapeamento CSV
e do contrato JSON.

Uso: python gerar_modelo.py [contrato.json]
"""
import csv
import json
import sys
from pathlib import Path


def nome_tabela(fonte):
    nome = f"{fonte['owner']}.{fonte['tabela']}"
    if fonte.get("dblink"):
        nome += f"@{fonte['dblink']}"
    return nome


def ler_mapeamento(caminho):
    with open(caminho, encoding="utf-8", newline="") as arquivo:
        return list(csv.DictReader(arquivo, delimiter=";"))


def expressao_coluna(linha):
    if linha["expressao"]:
        return linha["expressao"]
    return f"{linha['alias_origem']}.{linha['campo_origem']}"


def origem_join(join):
    agregacao = join.get("agregacao")
    if not agregacao:
        return nome_tabela(join)
    colunas = agregacao["chaves"] + [f"SUM({c}) AS {c}" for c in agregacao["soma"]]
    return (
        "(\n        SELECT " + ",\n               ".join(colunas)
        + f"\n          FROM {nome_tabela(join)}"
        + "\n         GROUP BY " + ", ".join(agregacao["chaves"])
        + "\n    )"
    )


def bloco_joins(joins):
    linhas = []
    for join in joins:
        condicoes = "\n       AND ".join(
            f"{c['esquerda']} {c['operador']} {c['direita']}" for c in join["condicoes"]
        )
        linhas.append(
            f"  -- regra {join['id']}: {join.get('observacao', join['tabela'])}\n"
            f"  {join['tipo']} JOIN {origem_join(join)} {join['alias']}\n"
            f"        ON {condicoes}"
        )
    return "\n".join(linhas)


def gerar_ddl(destino, mapeamento):
    colunas = ",\n".join(
        f"    {l['campo_destino']:<30} {l['tipo_destino']}" for l in mapeamento
    )
    comentarios = "\n".join(
        f"COMMENT ON COLUMN {destino['owner']}.{destino['tabela']}.{l['campo_destino']} "
        f"IS '{l['descricao'].replace(chr(39), chr(39) * 2)}';"
        for l in mapeamento
    )
    return f"CREATE TABLE {nome_tabela(destino)} (\n{colunas}\n);\n\n{comentarios}\n"


def gerar_carga(contrato, mapeamento):
    destino = nome_tabela(contrato["destino"])
    principal = contrato["fonte_principal"]
    nomes = [l["campo_destino"] for l in mapeamento]
    select = ",\n".join(
        f"           {expressao_coluna(l):<40} AS {l['campo_destino']}" for l in mapeamento
    )
    filtros = "\n       AND ".join(contrato["filtros"])

    desempates = [j for j in contrato["joins"] if j.get("ultimo_registro")]
    colunas_rank = "".join(
        f",\n           ROW_NUMBER() OVER (PARTITION BY {j['ultimo_registro']['particao']}"
        f" ORDER BY {', '.join(j['ultimo_registro']['ordem'])}) AS RN_{j['alias']}"
        for j in desempates
    )
    filtro_rank = " AND ".join(f"RN_{j['alias']} = 1" for j in desempates) or "1 = 1"

    return (
        f"DELETE FROM {destino}\n"
        f" WHERE DAT_FATURA >= TO_DATE(:P_MES_REF, 'YYYYMM')\n"
        f"   AND DAT_FATURA < ADD_MONTHS(TO_DATE(:P_MES_REF, 'YYYYMM'), 1);\n\n"
        f"INSERT /*+ APPEND */ INTO {destino} (\n    "
        + ",\n    ".join(nomes)
        + "\n)\nWITH BASE AS (\n    SELECT\n"
        + select
        + colunas_rank
        + f"\n      FROM {nome_tabela(principal)} {principal['alias']}\n"
        + bloco_joins(contrato["joins"])
        + f"\n     WHERE {filtros}\n)\nSELECT "
        + ",\n       ".join(nomes)
        + f"\n  FROM BASE\n WHERE {filtro_rank};\n\nCOMMIT;\n"
    )


def gerar_modelo(caminho_contrato):
    pasta = Path(caminho_contrato).parent
    contrato = json.loads(Path(caminho_contrato).read_text(encoding="utf-8"))
    mapeamento = ler_mapeamento(pasta / contrato["mapeamento_csv"])

    pendencias = "\n".join(f"--   - {p}" for p in contrato.get("pendencias", []))
    cabecalho = (
        f"-- Arquivo gerado por gerar_modelo.py a partir de {Path(caminho_contrato).name}"
        f" e {contrato['mapeamento_csv']}. Não edite à mão.\n"
        f"-- {contrato['descricao']}\n"
        f"-- Parâmetro: :P_MES_REF ({contrato['parametros']['P_MES_REF']['formato']})\n"
        f"-- Pendências do contrato:\n{pendencias}\n\n"
    )
    sql = (
        cabecalho
        + "-- 1. Estrutura da tabela destino (executar uma vez)\n"
        + gerar_ddl(contrato["destino"], mapeamento)
        + "\n-- 2. Carga mensal\n"
        + gerar_carga(contrato, mapeamento)
    )
    saida = pasta / contrato["saida_sql"]
    saida.write_text(sql, encoding="utf-8")
    return saida


if __name__ == "__main__":
    contrato = sys.argv[1] if len(sys.argv) > 1 else Path(__file__).with_name("contrato.json")
    print(f"Gerado: {gerar_modelo(contrato)}")
