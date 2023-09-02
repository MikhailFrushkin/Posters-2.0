import os
from datetime import datetime

import psycopg2

from config import dbname, user, password, host, machine_name, ready_path


def update_base_postgresql():
    db_params = {
        "host": host,
        "database": dbname,
        "user": user,
        "password": password
    }

    with psycopg2.connect(**db_params) as connection:
        # Создание таблицы, если она не существует
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS Update_base (
            machin VARCHAR,
            update_timestamp TIMESTAMP DEFAULT current_timestamp,
            arts INT
        );
        '''
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)

            cursor.execute("SELECT * FROM Update_base WHERE machin = %s;", (machine_name,))

            existing_record = cursor.fetchone()
            arts_value = len(os.listdir(ready_path))
            if existing_record:
                # Обновление существующей записи
                update_query = "UPDATE Update_base SET update_timestamp = %s, arts = %s WHERE machin = %s;"
                update_values = (datetime.now(), arts_value, machine_name)
                cursor.execute(update_query, update_values)
            else:
                # Вставка новой записи
                insert_query = "INSERT INTO Update_base (machin, update_timestamp, arts) VALUES (%s, %s, %s);"
                insert_values = (machine_name, datetime.now(), arts_value)
                cursor.execute(insert_query, insert_values)

        # Подтверждение изменений (commit) выполняется один раз
        connection.commit()


def orders_base_postgresql(orders):
    db_params = {
        "host": host,
        "database": dbname,
        "user": user,
        "password": password
    }

    # Создание подключения и контекстного менеджера
    with psycopg2.connect(**db_params) as connection:
        # Создание таблицы, если она не существует
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS orders (
            machin VARCHAR,
            art VARCHAR,
            type_list VARCHAR,
            page INT,
            name_file VARCHAR,
            update_timestamp TIMESTAMP DEFAULT current_timestamp
        );
        '''
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)

            insert_data_query = (
                "INSERT INTO orders (machin, art, type_list, page, name_file)"
                "VALUES (%s, %s, %s, %s, %s);")

            cursor.executemany(insert_data_query, orders)

        # Подтверждение изменений (commit) выполняется один раз
        connection.commit()


if __name__ == '__main__':
    update_base_postgresql()
    # db_params = {
    #     "host": host,
    #     "database": dbname,
    #     "user": user,
    #     "password": password
    # }
    # with psycopg2.connect(**db_params) as connection:
    #     # Удаление таблицы, если она существует
    #     with connection.cursor() as cursor:
    #         drop_table_query = "DROP TABLE IF EXISTS orders;"
    #         cursor.execute(drop_table_query)
    #         print('Таблица удалена')
