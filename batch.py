import paramiko
import psycopg2
import csv
import os
import time

# Параметры подключения к базе данных
hostname = '192.168.122.7'
username = 'postgres'
password = 'password'
database = 'labb03'



conn = None

# Пути к исходной и целевой директориям
source_dir = 'data/measurement'
target_dir = '/home/postgres/data/data/measurement'

def verify_database_info():
    global hostname, username, password, database, hostname_sftp, username_sftp, password_sftp
    
    print("Текущая информация о базе данных:")
    print(f"Хост: {hostname}")
    print(f"Пользователь: {username}")
    print(f"Пароль: {password}")
    print(f"База данных: {database}")
    
    choice = input("Это правильная информация для подключения к базе данных? (1 - Да, 2 - Нет): ")
    
    if choice == "2":
        hostname = input("Введите хост: ")
        username = input("Введите пользователя: ")
        password = input("Введите пароль: ")
        database = input("Введите имя базы данных: ")


def create_scheme(scheme_name):
    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {scheme_name};"
    cursor = conn.cursor()
    cursor.execute(create_schema_query)
    conn.commit()
    cursor.close()

def install_file_fdw(schema):
    cursor = conn.cursor()
    cursor.execute(f"CREATE EXTENSION IF NOT EXISTS file_fdw WITH SCHEMA {schema};")
    conn.commit()
    cursor.close()

def create_file_server(schema):
    cursor = conn.cursor()

    drop_server_query = "DROP SERVER IF EXISTS file_server CASCADE"
    cursor.execute(drop_server_query)
    conn.commit()

    create_server_query = f"""
        CREATE SERVER file_server
        FOREIGN DATA WRAPPER file_fdw;
    """
    cursor.execute(create_server_query)
    conn.commit()
    cursor.close()


def create_foreign_table(table_name, csv_file):
    cursor = conn.cursor()
    create_scheme('external')
    cursor.execute(f"DROP FOREIGN TABLE IF EXISTS external.{table_name}")
    create_table_query = f"""
        CREATE FOREIGN TABLE external.{table_name} (
            city INTEGER,
            mark TIMESTAMP WITHOUT TIME ZONE,
            temperature DOUBLE PRECISION
        )
        SERVER file_server
        OPTIONS (
            filename '{csv_file}',
            format 'csv',
            header 'true'
        );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()


def create_measurement_foreign_tables():
    measurements_dir = 'data/measurement'  # Путь к директории с файлами измерений

    # Получение списка файлов измерений в директории
    measurement_files = [f for f in os.listdir(measurements_dir) if f.endswith('.csv')]

    for file_name in measurement_files:
        # Извлечение датасета из имени файла
        dataset = file_name.replace('.csv', '')

        # Формирование имени таблицы
        table_name = f"measurement_{dataset}"

        # Полный путь к файлу CSV
        csv_file = os.path.join(measurements_dir, file_name)

        # Создание внешней таблицы для текущего файла измерений
        create_foreign_table(table_name, csv_file)

    print("Созданы внешние таблицы для файлов измерений")


# Функция для создания таблицы в PostgreSQL
def create_table(scheme ,table_name, columns):
    cursor = conn.cursor()
    drop_query = f"DROP TABLE IF EXISTS {scheme}.{table_name.format()} CASCADE;"
    cursor.execute(drop_query)
    conn.commit()
    # Формирование строки CREATE TABLE
    create_query = f"CREATE TABLE {scheme}.{table_name} ({', '.join(columns)})"
    
    # Создание таблицы
    cursor.execute(create_query)
    
    conn.commit()
    cursor.close()

# Функция для импорта данных из CSV в таблицу PostgreSQL
def import_csv_to_table(schema, csv_file, table_name):
    cursor = conn.cursor()
    
    with open(csv_file, 'r') as file:
        # Чтение CSV файла с помощью модуля csv
        csv_data = csv.reader(file)
        columns = next(csv_data)  # Получение списка столбцов
        
        # Генерация строки INSERT для каждой записи
        insert_query = f"INSERT INTO {schema}.{table_name} VALUES ({', '.join(['%s'] * len(columns))})"
        
        # Установка схемы
        cursor.execute(f"SET search_path TO {schema}")
        
        # Вставка данных в таблицу
        file.seek(0)  # Возвращение к началу файла
        next(csv_data)  # Пропуск заголовка файла
        cursor.copy_from(file, table_name, sep=',', null='', columns=columns)
        
    conn.commit()
    cursor.close()


def merge_all_scheme(schema, table_name):
    cur = conn.cursor()

    # Получение списка внешних таблиц из схемы "external"
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='external';")
    external_tables = cur.fetchall()

    # Объединение внешних таблиц и вставка данных в таблицу "data.mesurement"
    for table in external_tables:
        cur.execute(f"INSERT INTO {schema}.{table_name} SELECT * FROM external.{table[0]};")

    # Завершение транзакции и закрытие соединения
    conn.commit()
    cur.close()
    conn.close()



# Получение списка файлов CSV в папке "data/measurement/"
def get_csv_files():
    csv_files = []
    measurement_dir = 'data/measurement/'
    for file_name in os.listdir(measurement_dir):
        if file_name.endswith('.csv'):
            csv_files.append(os.path.join(measurement_dir, file_name))
    return csv_files

def get_connection():
    print("Начинаем соединение с БД")
    global conn
    attempts = 0
    max_attempts = 5
    delay = 2  # Задержка между попытками подключения (в секундах)
    
    while attempts < max_attempts:
        try:
            conn = psycopg2.connect(
                host=hostname,
                database=database,
                user=username,
                password=password
            )
            print("Успешное подключение к базе данных")
            break
        except psycopg2.Error as e:
            print("Ошибка подключения к базе данных:", e)
            attempts += 1
            print(f"Повторная попытка подключения через {delay} сек...")
            time.sleep(delay)

# Проверка правильности информации о базе данных
verify_database_info()

# Установка подключения
get_connection()

if conn is not None:
    print("Создаем схему data")
    create_scheme('data')

    install_file_fdw('data')
    create_file_server('data')

    print("Создаем таблицу регионов")
    # Создание таблицы "regions"
    create_table('data', 'regions', ['identifier SERIAL PRIMARY KEY', 'description TEXT'])

    print("Создаем таблицу стран")
    # Создание таблицы "countries"
    create_table('data', 'countries', ['identifier SERIAL PRIMARY KEY', 'region INTEGER REFERENCES data.regions(identifier)', 'description TEXT'])

    print("Создаем таблицу городов")
    # Создание таблицы "cities"
    create_table('data', 'cities', ['identifier SERIAL PRIMARY KEY', 'country INTEGER REFERENCES data.countries(identifier)', 'description TEXT', 'latitude DOUBLE PRECISION', 'longitude DOUBLE PRECISION', 'dataset TEXT'])

    print("Создаем таблицу измерений")
    # Создание таблицы "measurement"
    create_table('data','measurement', ['city INTEGER REFERENCES data.cities(identifier)', 'mark TIMESTAMP WITHOUT TIME ZONE', 'temperature DOUBLE PRECISION'])

    print("Создаем таблицу береговых линий")
    # Создание таблицы "coastline"
    create_table('data', 'coastline', ['shape INTEGER', 'segment INTEGER', 'latitude DOUBLE PRECISION', 'longitude DOUBLE PRECISION'])


   

    # Создание внешних таблиц измерений
    create_measurement_foreign_tables()

    print("Импорт данных в таблицу регионов из data/regions.csv")
    # Импорт данных в таблицу "regions"
    import_csv_to_table('data', 'data/regions.csv', 'regions')

    print("Импорт данных в таблицу стран из data/countries.csv")
    # Импорт данных в таблицу "countries"
    import_csv_to_table('data', 'data/countries.csv', 'countries')

    print("Импорт данных в таблицу городов из data/cities.csv")
    # Импорт данных в таблицу "cities"
    import_csv_to_table('data', 'data/cities.csv', 'cities')

    print("Импорт данных в таблицу береговых линий из data/coastline.csv")
    # Импорт данных в таблицу "coastline"
    import_csv_to_table('data', 'data/coastline.csv', 'coastline')

    print("Соединение всех внешних таблиц из external в measurement")
    merge_all_scheme('data', 'measurement')

    conn.close()
else:
    print("Ошибка подключения! Убедитесь, что вы правильно выбрали IP-адрес")
