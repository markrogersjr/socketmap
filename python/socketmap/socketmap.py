import os
import json
from socketmap.postgres import PostgresServer, PostgresClient


CLUSTER = 'main'
USER = 'postgres'
DATABASE = 'postgres'
SQL_CREATE_TABLE = 'CREATE TABLE "{table}" ({datatypes});'
SQL_INSERT_INTO = 'INSERT INTO "{table}" ({fields}) VALUES ({values});'
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


def create_foreach_wrapper(cluster, user, database, table, func):
    r'''Returns a function compatible with
    `pyspark.sql.DataFrame.foreachPartition` which applies
    `func`: pyspark.sql.Row -> dict to each row'''
    def wrapper(iterator):
        with PostgresClient(cluster, user, database) as client:
            for record in iterator:
                obj_string = json.dumps(func(record))
                client.execute(SQL_INSERT_INTO.format(
                    table=table,
                    fields=FIELD_NAME,
                    values=f"'{obj_string}'",
                ))
    return wrapper


def parse_json(row):
    r'''Simple helper function to parse JSON blobs in intermediate CSV file'''
    string = row[FIELD_NAME].strip('"').replace('""', '"')
    return json.loads(string)


def socketmap(spark, df, func, cluster=CLUSTER, user=USER, database=DATABASE):
    r'''Returns a `pyspark.sql.DataFrame` that is the result of applying
    `func`: pyspark.sql.Row -> dict to each record of `pyspark.sql.DataFrame`
    `df`'''
    table = 'socket2me'
    path = os.path.join('/tmp', table)
    with PostgresServer(cluster, user, database):
        with PostgresClient(cluster, user, database) as client:
            create_table(client, table)
            wrapper = create_foreach_wrapper(cluster, user, database,
                                             table, func)
            df.foreachPartition(wrapper)
            export_table(client, table, path)
    df = spark.read.option('header', True).csv(path)
    df = spark.createDataFrame(df.rdd.map(lambda row: parse_json(row)))
    return df
