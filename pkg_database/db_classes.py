from datetime import datetime
from mimetypes import init

def cabecalho(text):
    print("-" * len(text))
    print(text)
    print("-" * len(text))


class SqlServer:
    def __init__(self, table_name, connection, columns, primary_key, values):
        self.table_name = table_name
        self.cursor = connection.cursor()
        self.columns = columns
        self.col_names = [x[0] for x in self.columns]
        self.primary_key = primary_key
        self.values = values


    def create_table(self):
        cabecalho('Criando tabela no SQL Server')

        create_columns = ", ".join([f'{self.col_names[i]} {self.data_type_convertion(i)}' for i in range(len(self.columns))])

        try:
            self.cursor.execute(f"CREATE TABLE {self.table_name} ({create_columns}, HASH VARCHAR(255), OPERACAO VARCHAR(1))")
            self.cursor.execute(f"COMMIT")
            print(f"Tabela {self.table_name} criada com SUCESSO!")

        except Exception as e:
            print("ERRO: " + str(e))

    def insert_into(self):
        cabecalho(f'Inserindo registros na tabela {self.table_name}.')

        col_values = []
        
        for line in self.values:
            for x in range(len(line)):

                if line[x] == None:
                    col_values.append('NULL')

                elif type(line[x]) == datetime:
                    col_values.append(f"CAST('{line[x].strftime('%Y-%m-%d')}' as date)")
                    
                else:
                    col_values.append(line[x])
        
            col_values = tuple(col_values)
            
            values =  ', '.join([f'{x}' if 'CAST' in str(x) else str(x) if str(x).isnumeric() or str(x) == 'NULL' else f"'{str(x)}'"  for x in col_values])
            self.cursor.execute(f"INSERT INTO {self.table_name} values ({values})")
            
            col_values = list(col_values)
            col_values.clear()

        self.cursor.execute('COMMIT')          
    
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
        self.diff = None

    def update_pk_table(self):
        cabecalho("Atualizando tabela de PKs")
        pk = self.cursor.execute(f"SELECT acc.column_name FROM all_constraints ac, all_cons_columns acc WHERE ac.table_name = '{self.table_name}' AND ac.table_name = acc.table_name AND ac.constraint_type = 'P' AND ac.constraint_name = acc.constraint_name").fetchall()
        pk_column = "-".join([x[0] for x in pk])
        already_exists = self.get_pk()
        if already_exists:
            self.cursor.execute(f"UPDATE TB_OSRV_PK SET colunas_pk='{pk_column}' WHERE nome_tabela='{self.table_name}'")
        else:
            self.cursor.execute(f"INSERT INTO TB_OSRV_PK (NOME_TABELA, COLUNAS_PK) VALUES ('{self.table_name}', '{pk_column}')")
        self.cursor.execute("COMMIT")


    def get_pk(self):
        try:
            self.pk = self.cursor.execute(f"SELECT colunas_pk from TB_OSRV_PK WHERE nome_tabela = '{self.table_name}'").fetchall()[0][0]
            print(self.pk)
            return True
        except:
            print('Primary Key não encontrada!')
            return False

    def get_columns(self):
        print(f"SELECT column_name, data_type, data_length from USER_TAB_COLUMNS WHERE table_name = '{self.table_name}'")
        self.columns = self.cursor.execute(f"SELECT column_name, data_type, data_length from USER_TAB_COLUMNS WHERE table_name = '{self.table_name}'").fetchall()

    def create_hash_table(self):
        try:
            self.cursor.execute(f"SELECT * FROM HASH_{self.table_name}")
            print('Tabela de Hashes já existe!')
        except:
            cabecalho('Criando Tabela de Hashes')
            self.cursor.execute(f"CREATE TABLE HASH_{self.table_name} (REFERENCIA VARCHAR(255), OPERACAO VARCHAR(1), HASH VARCHAR(255))")
            
    def compare_to_hash_table(self):
        pk_list = self.pk.split('-')
        referencia = " || '-' || ".join([f'a.{x}' for x in pk_list])
        pk_list = ", ".join([x for x in pk_list])
        print(self.columns)
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
        delete = f"SELECT b.referencia as HASH_PK, {formatted_columns}, b.hash as HASH, {op_d} from HASH_{self.table_name} b left join {self.table_name} a on (b.referencia = {referencia}) AND b.operacao <> 'D' and {referencia} is null"
        query = f"{insert} union all {update} union all {delete}"
        print(query)
        self.diff = self.cursor.execute(query).fetchall()

    def insert_into_hash_table(self):
        for line in self.diff:
            print(f"INSERT INTO HASH_{self.table_name} VALUES ('{line[0]}', '{line[-1]}', '{line[-2]}')")
            self.cursor.execute(f"INSERT INTO HASH_{self.table_name} VALUES ('{line[0]}', '{line[-1]}', '{line[-2]}')")



