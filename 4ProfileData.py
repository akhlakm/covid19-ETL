#%%
import sqlite3
import pandas as pd

engine = sqlite3.connect('covid19.sqlite.db')

#%% Schema

def create_profile(conn):
    try:
        conn.execute("SELECT 1 FROM profile;")
    except sqlite3.OperationalError:
        sql = '''
        CREATE TABLE profile (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tblname TEXT NOT NULL,
            column  TEXT NOT NULL,
            info    TEXT NOT NULL,
            value   TEXT,
            desc    TEXT,
            date    DATE DEFAULT CURRENT_TIMESTAMP
        );
        '''
        print(sql)
        conn.execute(sql)
        conn.commit()


#%% Profile Aggregate

def profile_table(conn, table):
    cnames  = pd.read_sql(f'SELECT * FROM {table} LIMIT 1', engine).columns.values
    dtypes  = [str(i) for i in pd.read_sql(f'SELECT * FROM {table} LIMIT 1', engine).dtypes]

    # Row count
    sql = f'''
    INSERT INTO profile ('value', 'tblname', 'column', 'info', 'desc')
    SELECT COUNT(*), '{table}', 'all', 'row_count', 'number of rows'
    FROM {table};
    '''
    print(sql)
    conn.execute(sql)

    # Column count, values, types
    sql = f'''INSERT INTO profile
    ('value', 'tblname', 'column', 'info', 'desc') VALUES
    ('{len(cnames)}',       '{table}', 'all', 'col_count', 'number of columns'),
    ('{",".join(cnames)}',  '{table}', 'all', 'col_names', 'names of columns'),
    ('{",".join(dtypes)}',  '{table}', 'all', 'col_types', 'types of columns');
    '''
    print(sql)
    conn.execute(sql)

def profile_audit(conn, table):
    sql = f'''
    INSERT INTO profile ('value', 'tblname', 'column', 'info', 'desc')
    SELECT COUNT(DISTINCT(row)), '{table}', 'all', 'null_row_count', 'total number of rows with null or empty'
    FROM audit WHERE tblname = '{table}' AND (audit = 'null' OR audit = 'empty')
    UNION ALL
    SELECT count(), '{table}', column, 'unique_count', 'no of uniques'
    FROM audit WHERE tblname = '{table}' AND audit = 'unique'
    GROUP BY column
    UNION ALL
    SELECT count(*), '{table}', column, 'null_count', 'no of null or empty for column'
    FROM audit WHERE tblname = '{table}' AND (audit = 'null' OR audit = 'empty')
    GROUP BY column;
    '''
    print(sql)
    conn.execute(sql)

#%%
engine.execute('drop table profile;')
create_profile(engine)

for tabl in ['counties', 'census']:
    profile_table(engine, tabl)
    profile_audit(engine, tabl)

engine.commit()
