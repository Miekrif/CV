import os
import psycopg2
from psycopg2 import errors
from dotenv import load_dotenv
from datetime import datetime

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')

load_dotenv(dotenv_path)

host_name = os.environ['ip']
host_port = os.environ['port']
user_name = os.environ['user_name']
user_password = os.environ['user_password']
db_name = os.environ['db_name']


# make connect to DB
def create_connection(host_name, host_port, user_name, user_password, db_name):
    connection = None
    try:
        connection = psycopg2.connect(
            host=host_name,
            port=host_port,
            user=user_name,
            password=user_password,
            database=db_name
        )
        print("Connection to PostgreSQL DB successful")
    except errors and Exception as e:
        print(f"The error '{e}' occurred")

    return connection


# SQL-request for create new table
def create_table(count_time, msg, report):
    connection = create_connection(host_name, host_port, user_name, user_password, db_name)
    cursor = connection.cursor()
    try:
        print(count_time)
        create_table_query = f'''CREATE TABLE IF NOT EXISTS "{report}"
                                  (
                                  name_of_column time PRIMARY KEY NOT NULL
                                  );'''
        cursor.execute(create_table_query)
    except Exception as e:
        print('create_table', e)
    finally:
        connection.commit()
        connection.close()


#del old time table
def add_new_column_to_table(msg, report):
    connection = create_connection(host_name, host_port, user_name, user_password, db_name)
    cursor = connection.cursor()
    try:
        print('ALTER TABLE')
        for key in msg.keys():
            if key == 'stats':
                for key_stats in msg.get('stats'):
                    print(f"""ALTER TABLE "{report}" ADD "{key_stats}" int""")
                    list_of_name_keys = f"""ALTER TABLE "{report}" ADD "stats_{key_stats}" int"""
                    cursor.execute(list_of_name_keys)

            print(f"""ALTER TABLE "{report}" ADD "{key}" int""")
            list_of_name_keys = f"""ALTER TABLE "{report}" ADD "{key}" int """
            cursor.execute(list_of_name_keys)
        # connection.commit()
    except Exception as e:
        print(e)
    finally:
        connection.commit()
        connection.close()


def inster_into_table_one_day(msg, report):
    connection = create_connection(host_name, host_port, user_name, user_password, db_name)
    cursor = connection.cursor()
    try:
        name_of_column = datetime.now().strftime("%H:%M:%S")
        print(
            f'''INSERT INTO "{report}"
              (name_of_column)
              VALUES
              ('{name_of_column}')
                                   '''
        )
        insert_table_query = f'''INSERT INTO "{report}"
                              (name_of_column)
                              VALUES
                              ('{name_of_column}')
                                                   '''
        cursor.execute(insert_table_query)
        # connection.commit()
        for key, values in msg.items():
            print(key, values)
            if key == 'stats':
                print(key)
                if isinstance(key, dict):
                    for i_key in key.keys():
                        print(
                            '''
                              INSERT INTO "%s"
                              ("stats_%s")
                              VALUES
                              (%i)
                               ''' % (report, key,  key.get(i_key, 0))
                        )
                        insert_table_query = '''
                              INSERT INTO "%s"
                              ("stats_%s")
                              VALUES
                              (%i)
                               ''' % (report, key,  key.get(i_key, 0))
                        cursor.execute(insert_table_query)
                        # connection.commit()

                else:
                    continue
            else:
                print(
                    '''INSERT INTO "%s"
                      ("%s")
                      VALUES
                      (%i)
                       ''' % (report, key, values)
                )
                insert_table_query = '''INSERT INTO "%s"
                      ("%s")
                      VALUES
                      (%i)
                       ''' % (report, key, values)
                cursor.execute(insert_table_query)
            # connection.commit()

        # connection.commit()
    except Exception as e:
        print(e)
    # finally:
    #     connection.close()

def export_data(name):
    connection = create_connection(host_name, host_port, user_name, user_password, db_name)
    cursor = connection.cursor()
    try:
        print('enter to copy')
        print(f'''/tmp/databases/{name}/{name}_{datetime.now().strftime("%d_%m_%y")}.csv''')
        with open(f'/tmp/databases/{name}/{name}_{datetime.now().strftime("%d_%m_%y")}.csv', 'w+') as write_file:
            cursor.copy_to(write_file, table=f'{name}', null='0')
        print('exit from copy')
    except errors and Exception as e:
        print(e)


def del_data(name):
    connection = create_connection(host_name, host_port, user_name, user_password, db_name)
    cursor = connection.cursor()
    try:
        insert_table_query = f'''DROP TABLE {name};'''
        cursor.execute(insert_table_query)
    except errors and Exception as e:
        print(e)
        connection.close()
        pass


def start_connection(msg):
    connection = create_connection(host_name, host_port, user_name, user_password, db_name)
    try:
        report = datetime.now().strftime("%d/%m/%y")  # name of table
        # settings for connection
        # write to table
        # create table
        create_table('one_day_metrics', msg, report)
        add_new_column_to_table(msg, report)
        inster_into_table_one_day(msg, report)
        # close connection
        connection.commit()
        connection.close()
    except errors and Exception as e:
        print(e)