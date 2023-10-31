import psycopg2
from psycopg2 import OperationalError
from psycopg2.errors import DuplicateDatabase


# функция подключения к БД
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Подключение к PostgreSQL БД выполнено успешно")
    except OperationalError as e:
        print(f"The error connection '{e}' occurred")
    return connection

# подключение к БД в роли postgres
# connection = create_connection(
#     "postgres", "postgres", "12345", "localhost", "5432"
# )


# функция создания БД
def create_database(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("База данных успешно создана")
    except OperationalError as e:
        print(f"The error database '{e}' occurred")
    except DuplicateDatabase:
        print("База данных уже существует")


# # подключение к БД systems1
# connection = create_connection(
#     "systems1", "ponchik", "1221", "localhost", "5432"
# )
# # создание БД systems1
# create_database_query = "CREATE DATABASE systems1"
# create_database(connection, create_database_query)


# функция для обработки запросов SQL
def execute_query(connection, query):
    connection.commit()
    # connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        # print("Запрос выполнен успешно")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    # finally:
    #     if connection:
    #         cursor.close()
    #         connection.close()
    #         print("Соединение с PostgreSQL закрыто")

# SQL запрос для создания таблицы wordList
create_wordList_table = """
CREATE TABLE IF NOT EXISTS wordList(
rowid SERIAL NOT NULL PRIMARY KEY, 
word TEXT,
isFiltred BOOLEAN
);
"""

# SQL запрос для создания таблицы URLList
create_URLList_table = """
CREATE TABLE IF NOT EXISTS urllist(
rowid SERIAL NOT NULL PRIMARY KEY, 
URL TEXT
);
"""

# SQL запрос для создания таблицы linkBtwURL
create_linkBtwURL_table = """
CREATE TABLE IF NOT EXISTS linkBtwURL(
rowid SERIAL NOT NULL PRIMARY KEY, 
fk_FromURL_id INTEGER REFERENCES urllist (rowid),
fk_ToURL_id INTEGER REFERENCES urllist (rowid)
);
"""

# SQL запрос для создания таблицы wordLocation
create_wordLocation_table = """
CREATE TABLE IF NOT EXISTS wordLocation(
rowid SERIAL NOT NULL PRIMARY KEY, 
fk_word_id INTEGER REFERENCES wordList (rowid),
fk_URL_id INTEGER REFERENCES URLList (rowid),
location INTEGER
);
"""

# SQL запрос для создания таблицы linkWord
create_linkWord_table = """
CREATE TABLE IF NOT EXISTS linkWord(
rowid SERIAL NOT NULL PRIMARY KEY, 
fk_word_id INTEGER REFERENCES wordList (rowid),
fk_link_id INTEGER REFERENCES linkBtwURL (rowid)
);
"""
#
# # вызовы функции для создания таблиц
# execute_query(connection, create_wordList_table)
# execute_query(connection, create_URLList_table)
# execute_query(connection, create_linkBtwURL_table)
# execute_query(connection, create_wordLocation_table)
# execute_query(connection, create_linkWord_table)
