from datetime import datetime
from tqdm import tqdm


def cabecalho(text):
    print("-" * len(text))
    print(text)
    print("-" * len(text))

class SqlServer:
    def __init__(self, table_name, connection, columns, primary_key):
        self.table_name = table_name
        self.cursor = connection.cursor()
        self.columns = columns
        self.col_names = [x[0] for x in self.columns]
        self.primary_key = primary_key
        
    def create_table(self):
        cabecalho('Criando tabela no SQL Server')

        create_columns = ", ".join([f'{self.col_names[i]} {self.data_type_convertion(i)}' for i in range(len(self.columns))])

        try:
            self.cursor.execute(f"CREATE TABLE {self.table_name} ({create_columns} , OPERACAO VARCHAR(1), DATA_HORA DATETIME)")
            self.cursor.execute(f"COMMIT")
            print(f"Tabela {self.table_name} criada com SUCESSO!")

        except Exception as e:
            print("ERRO: " + str(e))

    def insert_into(self, values):
        self.cursor.fast_executemany = True
        cabecalho(f'Inserindo registros na tabela {self.table_name}.')
        data = []
        col_values = []
        binds = ", ".join('?' for x in range(len(values[0]) - 1))
        contador = 0
        for line in tqdm(values):
            
            line = list(line)
            line.pop(-2)
            line.pop(0)
            for x in range(len(line)):
                if line[x] == None:
                    col_values.append('NULL')

                elif type(line[x]) == datetime:
                    col_values.append(f"CAST('{line[x].strftime('%Y-%m-%d')}' as date)")
                    
                else:
                    col_values.append(line[x])
            col_values.append(datetime.now().strftime('%Y/%m/%d'))
          
            data.append(list(col_values))

            col_values.clear()
            contador += 1
            if contador % 10000 == 0:
                self.cursor.executemany(f"INSERT INTO {self.table_name} values ({binds})", data)
                data.clear()
        if len(data) > 0:
            self.cursor.executemany(f"INSERT INTO {self.table_name} values ({binds})", data)

        self.cursor.execute('COMMIT')
        del values          
        del col_values
        del data

    def data_type_convertion(self, i):

        if self.columns[i][1] == 'BFILE':
            return 'VARCHAR(255)'
        elif self.columns[i][1] == 'BINARY_FLOAT':
            return 'REAL'
        elif self.columns[i][1] == 'DATE':
            return 'DATETIME'
        elif self.columns[i][1] == 'TIMESTAMP':
            return 'DATETIME'
        elif self.columns[i][1] == 'BINARY_DOUBLE':
            return 'DOUBLE PRECISION'
        elif self.columns[i][1] == 'BLOB':
            return 'VARBINARY(max)'
        elif self.columns[i][1] == 'CHAR':
            return f'CHAR({self.columns[i][2]})'
        elif self.columns[i][1] == 'CHARACTER':
            return f'CHARACTER({self.columns[i][2]})'
        elif self.columns[i][1] == 'CLOB':
            return 'VARCHAR(masx)'
        elif self.columns[i][1] == 'DECIMAL':
            return f'DECIMAL({self.columns[i][2]})'
        elif self.columns[i][1] == 'DEC':
            return f'DEC({self.columns[i][2]})'
        elif self.columns[i][1] == 'DOUBLE PRECISION':
            return 'FLOAT'
        elif self.columns[i][1] == 'FLOAT':
            return 'FLOAT'
        elif self.columns[i][1] == 'INTEGER' or self.columns[i][1] == 'INT':
            return 'DECIMAL(38)'
        elif self.columns[i][1] == 'LONG':
            return 'VARCHAR(max)'
        elif self.columns[i][1] == 'LONG RAW':
            return 'VARBINARY(max)'
        elif self.columns[i][1] == 'NCHAR':
            return f'NCHAR({self.columns[i][2]})'
        elif self.columns[i][1] == 'NCHAR VARYING':
            return f'NVARCHAR({self.columns[i][2]})'
        elif self.columns[i][1] == 'NCLOB':
            return 'NVARCHAR(max)'
        elif self.columns[i][1] == 'NUMBER':
            return 'FLOAT'
        elif self.columns[i][1] == 'NUMERIC':
            return f'NUMERIC({self.columns[i][2]})'
        elif self.columns[i][1] == 'NVARCHAR2':
            return f'NVARCHAR({self.columns[i][2]})'
        elif self.columns[i][1] == 'RAW':
            return f'VARBINARY({self.columns[i][2]})'
        elif self.columns[i][1] == 'REAL':
            return 'FLOAT'
        elif self.columns[i][1] == 'ROWID':
            return 'CHAR(18)'
        elif self.columns[i][1] == 'SMALLINT':
            return 'DECIMAL(38)'
        elif self.columns[i][1] == 'UROWID':
            return f'VARCHAR({self.columns[i][2]})'
        elif self.columns[i][1] == 'VARCHAR':
            return f'VARCHAR({self.columns[i][2]})'
        elif self.columns[i][1] == 'VARCHAR2':
            return f'VARCHAR({self.columns[i][2]})'
        elif self.columns[i][1] == 'XMLTYPE':
            return 'XML'
        else:
            print(f'WARNING: TIPO {self.columns[i][1]} NÃO ENCONTRADO!')
            return self.columns[i][1]


class Oracle:
    def __init__(self, table_name, connection):
        self.table_name = table_name
        self.cursor = connection.cursor()
        self.pk = None
        self.columns = None
        
    def update_pk_table(self):
        cabecalho("Atualizando tabela de PKs")
        pk = self.cursor.execute(f"SELECT acc.column_name FROM all_constraints ac, all_cons_columns acc WHERE ac.table_name = '{self.table_name}' AND ac.table_name = acc.table_name AND ac.constraint_type = 'P' AND ac.constraint_name = acc.constraint_name").fetchall()
        pk_column = "-".join([x[0] for x in pk])
        already_exists = self.get_pk()
        if already_exists:
            self.cursor.execute(f"UPDATE TB_OSRV_PK SET colunas_pk='{pk_column}' WHERE nome_tabela='{self.table_name}'")
        else:
            data = []
            data.append(self.table_name)
            data.append(pk_column)
            self.cursor.execute(f"INSERT INTO TB_OSRV_PK (NOME_TABELA, COLUNAS_PK) VALUES (:1, :2)", data)
        self.cursor.execute("COMMIT")
        
    def get_pk(self):
        try:
            self.pk = self.cursor.execute(f"SELECT colunas_pk from TB_OSRV_PK WHERE nome_tabela = '{self.table_name}'").fetchall()[0][0]
            return True
        except:
            print('Primary Key não encontrada!')
            return False

    def get_columns(self):
        self.columns = self.cursor.execute(f"SELECT column_name, data_type, data_length from USER_TAB_COLUMNS WHERE table_name = '{self.table_name}'").fetchall()
        return self.columns
    def create_hash_table(self):
        try:
            self.cursor.execute(f"SELECT * FROM HASH_{self.table_name}")
            print('Tabela de Hashes já existe!')
        except:
            cabecalho('Criando Tabela de Hashes')
            self.cursor.execute(f"CREATE TABLE HASH_{self.table_name} (REFERENCIA VARCHAR(255), OPERACAO VARCHAR(1), HASH VARCHAR(255))")
            
    def compare_to_hash_table(self, lst):
        cabecalho('Comparando com tabela de hashes')
        pk_list = self.pk.split('-')
        referencia = " || '-' || ".join([f'a.{x}' for x in pk_list])
        pk_list = ", ".join([x for x in pk_list])
        col = [x[0] for x in self.columns]
        formatted_columns = ", ".join([x for x in col])
        cast_hash = " || ".join([f'a.{x}' for x in col])
        select = f"SELECT TO_CHAR({referencia}) as HASH_PK, {formatted_columns}, CAST (standard_hash (({cast_hash}), 'SHA256') AS VARCHAR(256)) HASH,"
        op_i = "'I' as operacao "
        op_u = "'U' as operacao "
        op_d = "'D' as operacao "
        from_ = f'FROM {self.table_name} a left join HASH_{self.table_name} b on ({referencia} = b.referencia)'
        insert = f"{select}{op_i}{from_} WHERE b.hash is null"
        update = f"{select}{op_u}{from_} WHERE b.hash <> (CAST (standard_hash (({cast_hash}), 'SHA256') AS VARCHAR(256)))"
        delete = f"SELECT b.referencia as HASH_PK, {formatted_columns}, b.hash as HASH, {op_d} from HASH_{self.table_name} b left join {self.table_name} a on (b.referencia = {referencia}) WHERE b.operacao <> 'D' and {referencia} is null"
        query = f"{insert} union all {update} union all {delete}"
        self.cursor.prepare(query)
        #print(query)
        lst = self.cursor.execute(query).fetchmany(100000)
        if lst == []:
            return 1
        else:
            return lst
        
    def update_hash_table(self, diff):
        cabecalho('Inserindo registros na tabela de hashes')
        data_i = []
        data_u = []
        data_d = []
        contador = 0
        for line in tqdm(diff):
            if line[-1] == 'I':
                data_i.append((line[0], line[-1], line[-2]))
                contador += 1
            elif line[-1] == 'U':
                data_u.append((line[-2], line[0]))
                contador += 1
            elif line[-1] == 'D':
                data_d.append((line[0]))
                contador += 1
            if contador % 10000 == 0:
                self.cursor.executemany(f"INSERT INTO HASH_{self.table_name} VALUES (:F1, :F2, :F3)", data_i)
                self.cursor.executemany(f"UPDATE HASH_{self.table_name} SET OPERACAO='U', HASH=:F4 WHERE REFERENCIA=:F5", data_u)
                self.cursor.executemany(f"DELETE FROM HASH_{self.table_name} where REFERENCIA=:F6", data_d)
                self.cursor.execute('COMMIT')
                data_i.clear()
                data_u.clear()
                data_d.clear()
        if len(data_i) > 0 or len(data_u) > 0 or len(data_d) > 0:
            if len(data_d) == 1:
                data_d = [data_d]
            self.cursor.executemany(f"INSERT INTO HASH_{self.table_name} VALUES (:S1, :S2, :S3)", data_i)
            self.cursor.executemany(f"UPDATE HASH_{self.table_name} SET OPERACAO='U', HASH=:S4 WHERE REFERENCIA=:S5", data_u)
            self.cursor.executemany(f"DELETE FROM HASH_{self.table_name} where REFERENCIA=:S6", data_d)
            self.cursor.execute('COMMIT')
        del diff
        del data_i
        del data_u
        del data_d
