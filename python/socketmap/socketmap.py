import os
import json
from uuid import uuid4
from socketmap.postgres import PostgresServer, PostgresClient


CLUSTER = 'main'
USER = 'postgres'
DATABASE = 'postgres'
SQL_CREATE_TABLE = 'CREATE TABLE "{table}" ({datatypes});'
SQL_INSERT_INTO = 'INSERT INTO "{table}" ({fields}) VALUES {values};'
SQL_COPY = '''COPY "{table}" to '{path}' DELIMITER ',' CSV HEADER;'''
SQL_DROP_TABLE = 'DROP TABLE "{table}";'
FIELD_NAME = 'row'


def create_table(client, table):
    r'''Use PostgreSQL `client` to submit a SQL query that creates a table with
    name `table`'''
    client.execute(SQL_CREATE_TABLE.format(
        table=table,
        datatypes=f'{FIELD_NAME} json not null',
    ))
    client.commit()


def export_table(client, table, path):
    r'''Use PostgreSQL `client` to export table with name `table` to specified
    local `path`'''
    client.execute(SQL_COPY.format(
        table=table,
        path=path,
    ))
    client.execute(SQL_DROP_TABLE.format(
        table=table,
    ))
    client.commit()


def escape_quotes(obj):
    r'''Escape single quotes for compatibility with SQL'''
    obj = json.loads(json.dumps(obj))
    if isinstance(obj, list):
        return [escape_quotes(x) for x in obj]
    if isinstance(obj, dict):
        return {escape_quotes(k): escape_quotes(v) for k, v in obj.items()}
    if isinstance(obj, str):
        return obj.replace("'", "''")
    return obj


def create_foreach_wrapper(cluster, user, database, table, func):
    r'''Returns a function compatible with
    `pyspark.sql.DataFrame.foreachPartition` which applies
    `func`: `iter[pyspark.sql.Row]` -> `list[dict]`'''
    def wrapper(iterator):
        with PostgresClient(cluster, user, database) as client:
            outputs = func(iterator)
            for i, output in enumerate(outputs):
                outputs[i] = json.dumps(escape_quotes(output))
            values = ', '.join(map(
                lambda output: ''.join(("('", output, "')")),
                outputs,
            ))
            if values:
                client.execute(SQL_INSERT_INTO.format(
                    table=table,
                    fields=FIELD_NAME,
                    values=values,
                ))
    return wrapper


def parse_json(row):
    r'''Simple helper function to parse JSON blobs in intermediate CSV file'''
    string = row[FIELD_NAME].strip('"').replace('""', '"')
    return json.loads(string)


def socketmap(spark, df, func, cluster=CLUSTER, user=USER, database=DATABASE):
    r'''Returns a `pyspark.sql.DataFrame` that is the result of applying
    `func`: `iter[pyspark.sql.Row]` -> `list[dict]` to each record of
    `pyspark.sql.DataFrame` `df`'''
    table = f't{uuid4().hex}'
    path = os.path.join('/tmp', table)
    with PostgresServer(cluster, user, database):
        with PostgresClient(cluster, user, database) as client:
            create_table(client, table)
            wrapper = create_foreach_wrapper(cluster, user, database,
                                             table, func)
            df.foreachPartition(wrapper)
            export_table(client, table, path)
    df = spark.read.option('header', True).csv(path, sep='{]')
    df = spark.createDataFrame(df.rdd.map(parse_json))
    return df
