import re
from config_oracle import *
from config_sqlserver import *
import cx_Oracle
import pyodbc
from datetime import datetime
from db_classes import Oracle, SqlServer



        


def create_table_sqlserver(connection, table_name, columns_list):
        print(f"Criando tabela {table_name} no SQL Server")
        create_statement = ", ".join([f"{x[0]} {x[1]}{'' if x[1] in ('INT','DATE', 'LONG', 'VARCHAR(max)', 'NVARCHAR2(max)', 'FLOAT') else f'({x[2]})'}" for x in columns_list])
        sqlserver_cursor = connection.cursor()
        sqlserver_cursor.execute(f"CREATE TABLE {table_name} ({create_statement}, HASH VARCHAR(256), OPERACAO VARCHAR(1))")
        sqlserver_cursor.execute(f"COMMIT")


def connect_oracle():
    connection = cx_Oracle.connect(f'{USER}/{PASSWD}@//{HOST}:{PORT}/{SID}')
    return connection


def get_table_info(oracle_connection, table_name):
    oracle_cursor = oracle_connection.cursor()
    columns_oracle = oracle_cursor.execute(f"SELECT column_name, data_type, data_length from USER_TAB_COLUMNS WHERE table_name = '{table_name}'").fetchall()
    columns_sqlserver = oracle_cursor.execute(f"SELECT column_name, CASE data_type WHEN 'NCLOB' THEN 'NVARCHAR2(max)' WHEN 'CLOB' THEN 'VARCHAR(max)' WHEN 'LONG' THEN 'INT' WHEN 'VARCHAR2' THEN 'VARCHAR' WHEN 'NUMBER' THEN 'INT' ELSE data_type END data_type, data_length from USER_TAB_COLUMNS where table_name = '{table_name.upper()}'").fetchall()
    return columns_oracle, columns_sqlserver

def catch_pk(table_name, connection):
    oracle_cursor = connection.cursor()
    print(f"SELECT acc.column_name FROM all_constraints ac, all_cons_columns acc WHERE ac.table_name = '{table_name}' AND ac.table_name = acc.table_name AND ac.constraint_type = 'P' AND ac.constraint_name = acc.constraint_name")
    pk = oracle_cursor.execute(f"SELECT acc.column_name FROM all_constraints ac, all_cons_columns acc WHERE ac.table_name = '{table_name}' AND ac.table_name = acc.table_name AND ac.constraint_type = 'P' AND ac.constraint_name = acc.constraint_name").fetchall()
    return pk[0][0]

def create_hashes(columns):
    column_name = []
    for column in columns:
        column_name.append(column[0])
        hash_changes = " || ".join([f'a.{x}' for x in column_name])
    return hash_changes, column_name

def select_statement(hash_changes, column_name, table_name, oracle_connection, pk):
    formatted_columns = ", ".join([x for x in column_name])

    select = f"SELECT {formatted_columns}, CAST (standard_hash (({hash_changes}), 'SHA256') AS VARCHAR(256)) HASH"
    delete = f"SELECT b.referencia as {formatted_columns}, b.HASH as HASH, 'D' operacao FROM OSRV_HASH_{table_name} b left join {table_name} a ON (b.referencia = a.{pk})WHERE b.operacao <> 'D' and a.{pk} is null"

    insert = f", 'I' operacao "
    update = f", 'U' operacao "

    from_ = f"FROM {table_name} a "

    where_insert = f"WHERE NOT EXISTS (SELECT 1 FROM OSRV_HASH_{table_name} b WHERE a.{pk} = b.referencia)"
    where_update = f"WHERE EXISTS (SELECT 1 FROM OSRV_HASH_{table_name} b WHERE b.referencia = a.{pk} AND b.hash <> CAST (standard_hash (({hash_changes}), 'SHA256') AS VARCHAR(256)))"

    cursor = oracle_connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM OSRV_HASH_{table_name}")
        print(f'Tabela de hash OSRV_HASH_{table_name} já existe.')
    except:
        create_hash_table(oracle_connection, table_name, pk, columns_oracle)
    finally:
        formatted_columns = ", ".join([x for x in column_name])
        query = (f"{select}{insert}{from_}{where_insert} union all {select}{update}{from_}{where_update} union all {delete}")
        print(query)
        select = cursor.execute(query).fetchall()
        return select

def insert_into_hash_table(hashes, oracle_connection, table_name):
    oracle_cursor = oracle_connection.cursor()
    print("Inserindo registros na tabela de hashes")
    for line in hashes:
        print(f"INSERT INTO OSRV_HASH_{table_name} values ('{line[0]}', '{line[-1]}', '{line[-2]}')")
        oracle_cursor.execute(f"INSERT INTO OSRV_HASH_{table_name} values ('{line[0]}', '{line[-1]}', '{line[-2]}')")
        print('INSERIDO')
    oracle_cursor.execute("commit")
    print()


def connect_sqlserver():
    connection = pyodbc.connect(f"DRIVER={DRIVER}; Server={SERVER}; Database={DATABASE}; UID={UID}; PWD={PASSWORD}; TrustServerCertificate=yes;")
    return connection

def insert_into_sqlserver(record_list, table_name, sqlserver_connection, cols):
    sqlserver_cursor = sqlserver_connection.cursor()
    try: 
        create_table_sqlserver(sqlserver_conn, table, columns_sqlserver)
    except:
        print('Tabela no SQL Server já existe!')
    
    new_line = []
    col_name = [x[0] for x in cols]
    col_name.append('HASH')
    col_name.append('OPERACAO')
    col_for_insert= []
    print("Inserindo registros no SQL Server")
    for line in record_list:
        for c in range(len(line)):
            if line[c] == None:
                pass
            else:
                if type(line[c]) == datetime:
                    new_line.append("".join(f"CAST('{line[c].strftime('%Y-%m-%d')}' as date)"))
                    col_for_insert.append(col_name[c])
                else:
                    new_line.append(line[c])
                    col_for_insert.append(col_name[c])
        new_line = tuple(new_line)
        cols = ", ".join(x for x in col_for_insert)
        values =  ', '.join([f'{x}' if 'CAST' in str(x) else str(x) if str(x).isnumeric() else f"'{str(x)}'"  for x in new_line])

        sqlserver_cursor.execute(f"INSERT INTO {table_name} ({cols}) values ({values})") 
        new_line = list(new_line)
        new_line.clear()
        col_for_insert = list(col_for_insert)
        col_for_insert.clear()
    sqlserver_cursor.execute('COMMIT')  

def create_hash_table(oracle_connection, table_name, pk, columns):
    print('Criando tabela de Hashes...')
    for col in columns:
        if pk == col[0]:
            data_type = col[1]
            data_length = col[2]
    create_statement = f"CREATE TABLE OSRV_HASH_{table_name} (REFERENCIA {data_type}({data_length}), OPERACAO VARCHAR(1), HASH VARCHAR(250))"
    oracle_cursor = oracle_connection.cursor()
    oracle_cursor.execute(create_statement)
    oracle_cursor.execute('COMMIT')

table = input('Digite o nome da tabela a ser inserida: ').upper()
oracle_conn = connect_oracle()
sqlserver_conn = connect_sqlserver()
#columns_oracle, columns_sqlserver = get_table_info(oracle_conn, table)
#pk = catch_pk(table, oracle_conn)
#hash_changes, column_names = create_hashes(columns_oracle)
#insert = select_statement(hash_changes, column_names, table, oracle_conn, pk)
#insert_into_hash_table(insert, oracle_conn, table)
#insert_into_sqlserver(insert, table, sqlserver_conn, columns_sqlserver)

#sqlserver = SqlServer(table, sqlserver_conn, columns_oracle, pk, insert)
#sqlserver.create_table()
#sqlserver.insert_into()


oracle = Oracle(table, oracle_conn)
oracle.get_pk()
oracle.create_hash_table()
oracle.get_columns()
oracle.compare_to_hash_table()
oracle.insert_into_hash_table()
