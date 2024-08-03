from pymongo.mongo_client import MongoClient
from urllib.parse import quote_plus as quote

class MongoConnect:
    def __init__(self,
                 cert_path: str,  # Путь до файла с сертификатом
                 user: str,  # Имя пользователя БД
                 pw: str,  # Пароль пользователя БД
                 host: str,  # Хост для подключения
                 rs: str,  # replica set
                 auth_db: str,  # БД для аутентификации
                 main_db: str  # БД с данными
                 ):
        self.user = user
        self.pw = pw
        self.hosts = [host]
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    def url(self) -> str:
        return 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=','.join(self.hosts),
            rs=self.replica_set,
            auth_src=self.auth_db)

    def client(self):
        return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db]