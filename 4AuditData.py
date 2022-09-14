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
        rowval  TEXT NOT NULL,
        date    DATE DEFAULT CURRENT_TIMESTAMP
    );
    '''
    print(sql)
    conn.execute(sql)
    conn.commit()

def null_rows(conn, table, column):
    sql = f'''
    INSERT INTO audit ('rowval', 'tblname', 'column', 'audit')
    SELECT id, '{table}', '{column}', 'null row index'
    FROM {table} WHERE {column} IS NULL;
    '''
    print(sql)
    conn.execute(sql)

    sql = f'''
    INSERT INTO audit ('rowval', 'tblname', 'column', 'audit')
    SELECT id, '{table}', '{column}', 'empty row index'
    FROM {table} WHERE {column} = '';
    '''
    print(sql)
    conn.execute(sql)

def unique_values(conn, table, column):
    sql = f'''
    INSERT INTO audit ('rowval', 'tblname', 'column', 'audit')
    SELECT DISTINCT({column}) as rowval, '{table}', '{column}', 'unique value'
    FROM {table} WHERE {column} IS NOT NULL;
    '''
    print(sql)
    conn.execute(sql)

def invalid_date(conn, table, column):
    sql = f'''
    INSERT INTO audit ('rowval', 'tblname', 'column', 'audit')
    SELECT id, '{table}', '{column}', 'invalid date'
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
    cols = pd.read_sql(f"""
        SELECT value
        FROM profile_summary
        WHERE tblname = '{tabl}' AND column = 'all' AND info = 'col_names'
    """, engine).iloc[0,0].split(",")

    dtypes = pd.read_sql(f"""
        SELECT value
        FROM profile_summary
        WHERE tblname = '{tabl}' AND column = 'all' AND info = 'col_types'
    """, engine).iloc[0,0].split(",")

    for col, dt in zip(cols, dtypes):
        print(tabl, col, dt)
        null_rows(engine, tabl, col)
        if dt == 'object':
            unique_values(engine, tabl, col)

    invalid_date(engine, 'counties', 'date')

engine.commit()
