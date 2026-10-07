"""
Raw -> Trusted orientado pelo contrato de dataset (configs/*.json).

Lê os arquivos CSV que chegaram na raw, aplica as regras do bloco "trusted"
do contrato e grava em parquet particionado, registrando as partições no
Glue Catalog.

Uso (spark-submit ou job Glue):
    spark-submit jobs/raw_to_trusted.py \
        --config s3://bucket/configs/franquia_consumo_config.json \
        --source s3://bucket/raw/claro_notf_pcrf_sms/ \
        --target s3://bucket/trusted/franquia_consumo/ \
        --file-pattern "CLARO_NOTF_PCRF_SMS_*.CSV"

Controle (dentro de --target, ignorado por Spark/Athena por começar com "_"):
    _control/processed_files  arquivos já processados (evita duplicar a cada 15 min)
    _control/execution_log    resumo de cada execução
    _rejected                 linhas rejeitadas com o motivo
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

SOURCE_FILE_COL = "nome_arquivo_origem"
LOAD_TS_COL = "dt_carga"
REASON_COL = "motivo_rejeicao"


def log(msg):
    print(f"[raw_to_trusted] {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} {msg}", flush=True)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True, help="Caminho do contrato JSON (local ou s3://)")
    p.add_argument("--source", required=True, help="Pasta da raw onde chegam os CSV")
    p.add_argument("--target", required=True, help="Pasta da tabela trusted")
    p.add_argument("--file-pattern", default="*", help="Glob dos arquivos dentro de --source")
    p.add_argument("--skip-catalog", action="store_true", help="Não registra tabela/partições no catálogo")
    # parse_known_args: o Glue injeta parâmetros próprios (--JOB_NAME etc.)
    args, _ = p.parse_known_args()
    return args


# --------------------------------------------------------------------------
# Hadoop FS helpers (funcionam com s3://, s3a://, hdfs:// e caminhos locais)
# --------------------------------------------------------------------------
def _fs(spark, path):
    jvm = spark._jvm
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    return jpath.getFileSystem(spark._jsc.hadoopConfiguration()), jpath


def path_exists(spark, path):
    fs, jpath = _fs(spark, path)
    return fs.exists(jpath)


def list_files(spark, source, pattern):
    fs, _ = _fs(spark, source)
    glob = spark._jvm.org.apache.hadoop.fs.Path(source.rstrip("/") + "/" + pattern)
    statuses = fs.globStatus(glob) or []
    return [
        {"file_path": s.getPath().toString(), "file_size": s.getLen(), "file_mtime": s.getModificationTime()}
        for s in statuses
        if s.isFile()
    ]


def read_first_line(spark, path, encoding):
    jvm = spark._jvm
    fs, jpath = _fs(spark, path)
    reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(fs.open(jpath), encoding))
    try:
        return reader.readLine()
    finally:
        reader.close()


def load_config(spark, path):
    text = spark.read.text(path, wholetext=True).first()[0]
    return json.loads(text)


# --------------------------------------------------------------------------
# Regras
# --------------------------------------------------------------------------
def select_new_files(spark, files, control_path, allow_reprocess):
    """Remove os arquivos que já passaram pelo job.

    PROCESSED: com allow_reprocess=false nunca volta; com true, volta só se
    tamanho ou data de modificação mudaram.
    REJECTED: volta só se o arquivo foi substituído (tamanho/data mudaram).
    """
    if not files or not path_exists(spark, control_path):
        return files
    rows = spark.read.parquet(control_path).select("file_path", "file_size", "file_mtime", "status").distinct().collect()
    processed_paths = {r.file_path for r in rows if r.status == "PROCESSED"}
    seen_signatures = {(r.file_path, r.file_size, r.file_mtime) for r in rows}
    selected = []
    for f in files:
        if (f["file_path"], f["file_size"], f["file_mtime"]) in seen_signatures:
            continue
        if not allow_reprocess and f["file_path"] in processed_paths:
            continue
        selected.append(f)
    return selected


def validate_headers(spark, files, raw_cfg):
    """Confere o cabeçalho de cada arquivo contra expected/required_columns.

    Retorna (arquivos aceitos agrupados por cabeçalho, arquivos rejeitados).
    """
    delimiter = raw_cfg["delimiter"]
    if delimiter.upper() == "TAB":
        delimiter = "\t"
    expected = raw_cfg.get("expected_columns") or []
    required = raw_cfg.get("required_columns") or []
    groups, rejected = {}, []
    for f in files:
        line = read_first_line(spark, f["file_path"], raw_cfg.get("encoding") or "utf-8")
        header = tuple(c.strip() for c in (line or "").lstrip("\ufeff").split(delimiter))
        missing_required = [c for c in required if c not in header]
        if missing_required:
            log(f"REJEITADO {f['file_path']}: colunas obrigatórias ausentes {missing_required}")
            rejected.append({**f, "message": f"colunas obrigatórias ausentes: {missing_required}"})
            continue
        missing_expected = [c for c in expected if c not in header]
        if missing_expected:
            log(f"WARNING {f['file_path']}: colunas esperadas ausentes {missing_expected}")
        groups.setdefault(header, []).append(f)
    return groups, rejected, delimiter


def read_csv_groups(spark, groups, raw_cfg, delimiter):
    """Lê todos os arquivos como texto (sem inferência), um grupo por cabeçalho."""
    frames = []
    for header, files in groups.items():
        schema = T.StructType([T.StructField(c, T.StringType(), True) for c in header])
        df = (
            spark.read.option("header", str(raw_cfg.get("header", True)).lower())
            .option("sep", delimiter)
            .option("encoding", raw_cfg.get("encoding") or "utf-8")
            .option("multiLine", str(raw_cfg.get("multiline", False)).lower())
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .csv([f["file_path"] for f in files])
        )
        frames.append(df)
    df = frames[0]
    for other in frames[1:]:
        df = df.unionByName(other, allowMissingColumns=True)
    return df.withColumn(SOURCE_FILE_COL, F.input_file_name())


def format_to_regex(fmt):
    """yyyyMMddHHmmss -> ^\\d{14}$ ; dd.MM.yyyy -> ^\\d{2}\\.\\d{2}\\.\\d{4}$"""
    out = []
    for ch in fmt:
        out.append(r"\d" if ch.isalpha() else re.escape(ch))
    return "^" + "".join(out) + "$"


def add_derived_partitions(df, derived):
    """Cria as colunas de partição extraindo a parte pedida da coluna de origem.

    Ex.: DATA_EVENTO=20260729164941, extract=MM -> mes_evento='07'.
    Retorna o dataframe e uma expressão booleana de validade da origem.
    """
    valid = F.lit(True)
    for name, spec in derived.items():
        src, fmt, part = spec["source_column"], spec["source_format"], spec["extract"]
        pos = fmt.find(part)
        if pos < 0:
            raise ValueError(f"derived_partitions.{name}: '{part}' não existe em '{fmt}'")
        df = df.withColumn(name, F.substring(F.col(src), pos + 1, len(part)))
        valid = valid & F.col(src).rlike(format_to_regex(fmt))
        if part == "MM":
            valid = valid & F.col(name).cast("int").between(1, 12)
        elif part == "dd":
            valid = valid & F.col(name).cast("int").between(1, 31)
    return df, valid


def apply_trusted_rules(df, cfg):
    raw_cfg, trusted = cfg["raw"], cfg["trusted"]
    data_cols = [c for c in df.columns if c != SOURCE_FILE_COL]

    if trusted.get("trim_strings"):
        df = df.select(*[F.trim(F.col(c)).alias(c) if c in data_cols else F.col(c) for c in df.columns])
    if trusted.get("empty_to_null"):
        df = df.select(*[F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c) if c in data_cols else F.col(c) for c in df.columns])
    if trusted.get("drop_fully_empty_rows"):
        df = df.dropna(how="all", subset=data_cols)

    df, partition_valid = add_derived_partitions(df, trusted.get("derived_partitions") or {})

    # motivo da rejeição: a primeira regra que falhar
    reasons = []
    for c in raw_cfg.get("required_columns") or []:
        reasons.append((F.col(c).isNull(), f"coluna obrigatória nula: {c}"))
    if trusted.get("remove_null_primary_key"):
        for c in trusted.get("primary_key") or []:
            reasons.append((F.col(c).isNull(), f"chave primária nula: {c}"))
    if trusted.get("derived_partitions"):
        reasons.append((~partition_valid | partition_valid.isNull(), "valor inválido para partição"))
    reason = F.lit(None).cast("string")
    for cond, msg in reversed(reasons):
        reason = F.when(cond, F.lit(msg)).otherwise(reason)
    df = df.withColumn(REASON_COL, reason)

    rejected = df.filter(F.col(REASON_COL).isNotNull())
    valid = df.filter(F.col(REASON_COL).isNull()).drop(REASON_COL)

    for col_name, dtype in (trusted.get("schema_override") or {}).items():
        if col_name in valid.columns:
            valid = valid.withColumn(col_name, F.col(col_name).cast(dtype))

    return valid, rejected


def drop_already_loaded(spark, df, target, partition_cols, keys):
    """Remove linhas que já existem na trusted (mesmas partições), para a carga append."""
    if not keys or not path_exists(spark, target):
        return df
    try:
        existing = spark.read.parquet(target)
    except Exception:  # pasta existe mas ainda sem dados
        return df
    if partition_cols:
        touched = df.select(*partition_cols).distinct()
        existing = existing.join(F.broadcast(touched), partition_cols, "inner")
    return df.join(existing.select(*keys).distinct(), keys, "left_anti")


def write_trusted(df, target, trusted, partition_cols):
    strategy = trusted.get("load_strategy", "append")
    if partition_cols:
        # um arquivo por partição a cada execução (evita milhares de arquivos pequenos)
        df = df.repartition(*partition_cols)
    writer = df.write.option("compression", trusted.get("compression") or "snappy")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    if strategy == "overwrite_partition":
        df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        writer = writer.mode("overwrite")
    elif strategy == "overwrite":
        df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        writer = writer.mode("overwrite")
    else:
        writer = writer.mode("append")
    writer.parquet(target)


def register_catalog(spark, cfg, df, target, partition_cols, partitions):
    db = cfg["catalog"]["trusted_database"]
    table = cfg["metadata"]["dataset_name"]
    data_cols = [f for f in df.schema.fields if f.name not in partition_cols]
    cols_ddl = ", ".join(f"`{f.name.lower()}` {f.dataType.simpleString()}" for f in data_cols)
    part_ddl = ", ".join(f"`{c}` string" for c in partition_cols)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS `{db}`.`{table}` ({cols_ddl}) "
        + (f"PARTITIONED BY ({part_ddl}) " if partition_cols else "")
        + f"STORED AS PARQUET LOCATION '{target}'"
    )
    for values in partitions:
        spec = ", ".join(f"`{c}`='{values[c]}'" for c in partition_cols)
        loc = target.rstrip("/") + "/" + "/".join(f"{c}={values[c]}" for c in partition_cols)
        spark.sql(f"ALTER TABLE `{db}`.`{table}` ADD IF NOT EXISTS PARTITION ({spec}) LOCATION '{loc}'")
    log(f"catálogo atualizado: {db}.{table} (+{len(partitions)} partição(ões))")


def save_control(spark, path, rows, schema):
    if rows:
        spark.createDataFrame(rows, schema).write.mode("append").parquet(path)


CONTROL_SCHEMA = T.StructType([
    T.StructField("file_path", T.StringType()),
    T.StructField("file_size", T.LongType()),
    T.StructField("file_mtime", T.LongType()),
    T.StructField("status", T.StringType()),
    T.StructField("message", T.StringType()),
    T.StructField("processed_at", T.TimestampType()),
])

EXECUTION_SCHEMA = T.StructType([
    T.StructField("dataset_name", T.StringType()),
    T.StructField("started_at", T.TimestampType()),
    T.StructField("finished_at", T.TimestampType()),
    T.StructField("files_found", T.LongType()),
    T.StructField("files_processed", T.LongType()),
    T.StructField("files_rejected", T.LongType()),
    T.StructField("rows_loaded", T.LongType()),
    T.StructField("rows_rejected", T.LongType()),
    T.StructField("rows_duplicated", T.LongType()),
    T.StructField("partitions", T.StringType()),
])


def main():
    args = parse_args()
    started = datetime.now(timezone.utc).replace(tzinfo=None)

    builder = SparkSession.builder.appName("raw_to_trusted")
    if not args.skip_catalog:
        builder = builder.enableHiveSupport()
    spark = builder.getOrCreate()

    cfg = load_config(spark, args.config)
    meta, raw_cfg, trusted, audit = cfg["metadata"], cfg["raw"], cfg["trusted"], cfg.get("audit", {})
    dataset = meta["dataset_name"]

    if not (cfg.get("enabled") and cfg["processing"].get("active") and trusted.get("enabled")):
        log(f"{dataset}: dataset/processamento/trusted desabilitado no contrato, nada a fazer")
        return
    if raw_cfg.get("file_format") != "csv":
        raise ValueError(f"file_format '{raw_cfg.get('file_format')}' não suportado por este job (apenas csv)")

    target = args.target.rstrip("/")
    control_path = f"{target}/_control/processed_files"
    exec_path = f"{target}/_control/execution_log"
    rejected_path = f"{target}/_rejected"
    partition_cols = trusted.get("partition_columns") or []
    track_files = audit.get("enabled", True) and audit.get("track_processed_files", True)

    found = list_files(spark, args.source, args.file_pattern)
    files = select_new_files(spark, found, control_path, cfg["processing"].get("allow_reprocess")) if track_files else found
    log(f"{dataset}: {len(found)} arquivo(s) encontrado(s), {len(files)} novo(s)")
    if not files:
        return

    groups, bad_files, delimiter = validate_headers(spark, files, raw_cfg)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    control_rows = [
        (f["file_path"], f["file_size"], f["file_mtime"], "REJECTED", f["message"], now) for f in bad_files
    ]

    loaded = rejected_count = duplicated = 0
    partitions = []
    if groups:
        df = read_csv_groups(spark, groups, raw_cfg, delimiter)
        valid, rejected = apply_trusted_rules(df, cfg)

        keys = trusted.get("deduplication_keys") if trusted.get("deduplicate") else []
        before = valid.cache().count()
        if keys:
            valid = valid.dropDuplicates(keys)
            if trusted.get("load_strategy", "append") == "append":
                valid = drop_already_loaded(spark, valid, target, partition_cols, keys)
        valid = valid.withColumn(LOAD_TS_COL, F.lit(now).cast("timestamp")).cache()
        loaded = valid.count()
        duplicated = before - loaded

        if partition_cols:
            partitions = [r.asDict() for r in valid.select(*partition_cols).distinct().collect()]

        if loaded:
            write_trusted(valid, target, trusted, partition_cols)
            log(f"{dataset}: {loaded} linha(s) gravada(s) em {target} partições={partitions}")

        rejected_count = rejected.count()
        if rejected_count:
            log(f"{dataset}: {rejected_count} linha(s) rejeitada(s)")
            if audit.get("save_rejected_records", True):
                rejected.withColumn(LOAD_TS_COL, F.lit(now).cast("timestamp")).write.mode("append").parquet(rejected_path)

        if loaded and not args.skip_catalog and cfg.get("catalog", {}).get("create_glue_table"):
            register_catalog(spark, cfg, valid, target, partition_cols, partitions)

        for header_files in groups.values():
            control_rows += [
                (f["file_path"], f["file_size"], f["file_mtime"], "PROCESSED", None, now) for f in header_files
            ]

    if track_files:
        save_control(spark, control_path, control_rows, CONTROL_SCHEMA)
    if audit.get("enabled", True) and audit.get("save_execution_log", True):
        finished = datetime.now(timezone.utc).replace(tzinfo=None)
        save_control(spark, exec_path, [(
            dataset, started, finished, len(found), len(files) - len(bad_files), len(bad_files),
            loaded, rejected_count, duplicated, json.dumps(partitions),
        )], EXECUTION_SCHEMA)

    log(f"{dataset}: concluído — carregadas={loaded} rejeitadas={rejected_count} duplicadas={duplicated} "
        f"arquivos_rejeitados={len(bad_files)}")
    if bad_files and not groups:
        sys.exit(1)


if __name__ == "__main__":
    main()
