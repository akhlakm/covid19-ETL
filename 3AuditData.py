#%%
import sqlite3
import pandas as pd

engine = sqlite3.connect('covid19.sqlite.db')

#%% Schema
def create_audit(conn):
    try:
        conn.execute("DROP TABLE audit;")
    except sqlite3.OperationalError:
        pass

    sql = '''
    CREATE TABLE audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tblname TEXT NOT NULL,
        column  TEXT NOT NULL,
        audit   TEXT NOT NULL,
        row     INTEGER,
        value   TEXT,
        desc    TEXT,
        date    DATE DEFAULT CURRENT_TIMESTAMP
    );
    '''
    print(sql)
    conn.execute(sql)
    conn.commit()


#%% Profile

def profile_table(conn, table):
    cnames  = pd.read_sql(f'SELECT * FROM {table} LIMIT 1', engine).columns.values
    dtypes  = [str(i) for i in pd.read_sql(f'SELECT * FROM {table} LIMIT 1', engine).dtypes]

    # Row count
    sql = f'''
    INSERT INTO audit ('value', 'tblname', 'column', 'audit', 'desc')
    SELECT COUNT(*), '{table}', 'all', 'row_count', 'number of rows'
    FROM {table};
    '''
    print(sql)
    conn.execute(sql)

    # Column count, values, types
    sql = f'''INSERT INTO audit
    ('value', 'tblname', 'column', 'audit', 'desc') VALUES
    ('{len(cnames)}',       '{table}', 'all', 'col_count', 'number of columns'),
    ('{",".join(cnames)}',  '{table}', 'all', 'col_names', 'names of columns'),
    ('{",".join(dtypes)}',  '{table}', 'all', 'col_types', 'types of columns');
    '''
    print(sql)
    conn.execute(sql)

def null_rows(conn, table, column):
    sql = f'''
    INSERT INTO audit ('row', 'tblname', 'column', 'audit', 'desc')
    SELECT id, '{table}', '{column}', 'null', 'null row index'
    FROM {table} WHERE {column} IS NULL UNION ALL
    SELECT id, '{table}', '{column}', 'empty', 'empty row index'
    FROM {table} WHERE {column} = '';
    '''
    print(sql)
    conn.execute(sql)

def unique_values(conn, table, column):
    sql = f'''
    INSERT INTO audit ('value', 'tblname', 'column', 'audit', 'desc')
    SELECT DISTINCT({column}), '{table}', '{column}', 'unique', 'unique value'
    FROM {table} WHERE {column} IS NOT NULL;
    '''
    print(sql)
    conn.execute(sql)

def invalid_date(conn, table, column):
    sql = f'''
    INSERT INTO audit ('row', 'value', 'tblname', 'column', 'audit', 'desc')
    SELECT id, {column}, '{table}', '{column}', 'invalid_date', 'invalid date row and value'
    FROM {table}
    WHERE DATE(substr({column},7,4)
    ||'-'
    ||substr({column},4,2)
    ||'-'
    ||substr({column},1,2)) 
    NOT BETWEEN DATE('2019-01-01') AND DATE('now');
    '''
    print(sql)
    conn.execute(sql)

#%% Run
create_audit(engine)

for tabl in ['counties', 'census']:
    profile_table(engine, tabl)

    cnames  = pd.read_sql(f'SELECT * FROM {tabl} LIMIT 1', engine).columns.values
    dtypes  = [str(i) for i in pd.read_sql(f'SELECT * FROM {tabl} LIMIT 1', engine).dtypes]

    for col, dt in zip(cnames, dtypes):
        print(tabl, col, dt)
        null_rows(engine, tabl, col)
        if dt == 'object':
            unique_values(engine, tabl, col)

    invalid_date(engine, 'counties', 'date')

engine.commit()
