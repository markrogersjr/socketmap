from subprocess import call
from traceback import print_exception
import psycopg2


CTL_CLUSTER = 'pg_ctlcluster 13 {cluster} {action}'


class PostgresServer:

    def ctl(self, action):
        r'''base method for controlling PostgreSQL clusters'''
        cmd = CTL_CLUSTER.format(cluster=self.cluster, action=action)
        call(cmd, shell=True)

    def start(self):
        r'''Start the PostgreSQL server'''
        self.ctl('start')

    def stop(self):
        r'''Stop the PostgreSQL server'''
        self.ctl('stop')

    def __init__(self, cluster, user, database):
        self.cluster = cluster
        self.user = user
        self.database = database
        self.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        if exc_type is not None:
            print_exception(exc_type, exc_value, traceback)
        return True


class PostgresClient:

    def commit(self):
        r'''Snapshot the database so that changes will persist upon restarting
        the cluster'''
        self.connection.commit()

    def start(self):
        r'''Connect to the PostgreSQL server to submit SQL commands'''
        self.connection = psycopg2.connect(
            f'dbname={self.database} user={self.user} host=localhost password=postgres port=5432',
        )
        self.cursor = self.connection.cursor()

    def stop(self):
        r'''Close the connection to the PostgreSQL server'''
        self.commit()
        self.cursor.close()
        self.connection.close()

    def __init__(self, cluster, user, database):
        self.cluster = cluster
        self.user = user
        self.database = database
        self.start()

    def execute(self, sql_statement):
        r'''Submit a SQL command'''
        self.cursor.execute(sql_statement)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        if exc_type is not None:
            print_exception(exc_type, exc_value, traceback)
        return True
