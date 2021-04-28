from subprocess import call
from traceback import print_exception
import psycopg2


CTL_CLUSTER = 'pg_ctlcluster 13 {cluster} {action}'


class PostgresServer:

    def ctl(self, action):
        cmd = CTL_CLUSTER.format(cluster=self.cluster, action=action)
        call(cmd, shell=True)

    def start(self):
        self.ctl('start')

    def stop(self):
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
        self.connection.commit()

    def start(self):
        self.connection = psycopg2.connect(
            f'dbname={self.database} user={self.user} host=localhost password=postgres port=5432',
        )
        self.cursor = self.connection.cursor()
#        print('\n\n\nCONNECTION')
#        for k in dir(self.connection):
#            print(f'{k} -> {getattr(self.connection, k)}')
#        print('\nCURSOR')
#        for k in dir(self.cursor):
#            print(f'{k} -> {getattr(self.cursor, k)}')

    def stop(self):
        self.commit()
        self.cursor.close()
        self.connection.close()

    def __init__(self, cluster, user, database):
        self.cluster = cluster
        self.user = user
        self.database = database
        self.start()

    def execute(self, sql_statement):
        self.cursor.execute(sql_statement)


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        if exc_type is not None:
            print_exception(exc_type, exc_value, traceback)
        return True
