import psycopg2
from contextlib import closing


class Connection:
    """ Подключение к базе данных с использованием DB API2 """

    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

        self.db = None  # атрибут подключения к базе данных
        self.query_collector = None  # буфер для результатов выборки
        self.err = ''  # буфер для фиксации ошибок
        self.cursor = None

    def connection(self):
        """ Подключение к базе данных """
        try:
            self.db = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port)
            self.cursor = self.db.cursor()
        except Exception as err:
            raise Exception("Ошибка подключения к базе данных: %s" % err.message)

    def disconnection(self):
        self.db.close()
        self.cursor.close()

    def send_query(self, query):
        """ Выполнение SELECT-запросов """
        self.cursor.execute(query)

    def fetch_all(self):
        self.cursor.fetcheall()