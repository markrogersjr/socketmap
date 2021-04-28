import os
import json
from psycopg2 import sql
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
    client.execute(SQL_CREATE_TABLE.format(
        table=table,
        datatypes=f'{FIELD_NAME} json not null',
    ))
    client.commit()


def export_table(client, table, path):
    client.execute(SQL_COPY.format(
        table=table,
        path=path,
    ))
    client.execute(SQL_DROP_TABLE.format(
        table=table,
    ))
    client.commit()


def create_foreach_wrapper(cluster, user, database, table, func):
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
    string = row[FIELD_NAME].strip('"').replace('""', '"')
    return json.loads(string)


def socketmap(spark, df, func, cluster=CLUSTER, user=USER, database=DATABASE):
    table = 'socket2me'
    path = os.path.join('/tmp', table)
    with PostgresServer(cluster, user, database) as server:
        with PostgresClient(cluster, user, database) as client:
            create_table(client, table)
            wrapper = create_foreach_wrapper(
                cluster,
                user,
                database,
                table,
                func,
            )
            df.foreachPartition(wrapper)
            export_table(client, table, path)
    df = spark.read.option('header', True).csv(path)
    df = spark.createDataFrame(df.rdd.map(lambda row: parse_json(row)))
    return df
