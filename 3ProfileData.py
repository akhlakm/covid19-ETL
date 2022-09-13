#%%
import sqlite3
import pandas as pd

engine = sqlite3.connect('covid19.sqlite.db')

#%% Schema
def create_profile(conn):
    schema = ['table', 'column', 'info', 'value', 'profilev']
    df = pd.DataFrame(columns = schema)
    df = df.astype({'profilev': str})
    df.to_sql('profile_summary', con=conn, if_exists='replace', index=False)

#%% Profile
def profile(table, col, info, value):
    sql = ('INSERT INTO profile_summary'
        f'("table", "column", "info", "value", "profilev")'
        f'VALUES ("{table}", "{col}", "{info}", "{value}", DATE("now"));'
    )
    print(sql)
    engine.execute(sql)

def profile_row_counts(tablename):
    v = pd.read_sql(f'SELECT COUNT(1) FROM {tablename}', engine).iloc[0,0]
    profile(tablename, 'all', 'row_count', v)

def col_names(tablename):
    return pd.read_sql(f'SELECT * FROM {tablename} LIMIT 1', engine).columns.values

def data_types(tablename):
    return pd.read_sql(f'SELECT * FROM {tablename} LIMIT 1', engine).dtypes

def profile_col_count(tablename):
    profile(tablename, 'all', 'col_count', len(col_names(tablename)))

def profile_col_names(tablename):
    values = ",".join(col_names(tablename))
    profile(tablename, 'all', 'col_names', values)

def profile_data_types(tablename):
    values = ",".join([str(i) for i in data_types(tablename)])
    profile(tablename, 'all', 'col_types', values)

def profile_null(table):
    # for col in col_names(table):
    #     sql = f'''SELECT COUNT({col})
    #         FROM {table} WHERE {col} IS NULL;
    #     '''
    #     profile(table, col, 'null_count', pd.read_sql(sql, engine).iloc[0,0])
    sqls = []
    for col in col_names(table):
        sqls.append(f'''
            SELECT '{col}' as column, COUNT({col}) as value
            FROM {table}
            WHERE {col} IS NULL
            ''')
    sql = "UNION ALL".join(sqls)
    df = pd.read_sql(sql, engine).assign(table = table, info = 'null_count')
    df.to_sql('profile_summary', con=engine, if_exists='append', index=False)

def profile_numeric_columns(table):
    columns = col_names(table)
    dtypes = data_types(table)

    for col, dt in zip(columns, dtypes):
        if dt == float:
            sql = f'''
            SELECT
                MIN({col}) AS col_min, 
                MAX({col}) AS col_max, 
                AVG({col}) AS col_avg
            FROM {table}
            WHERE {col} IS NOT NULL;
            '''

        elif dt == int or dt == str or dt == object:
            sql = f'''
            SELECT
                MIN({col}) AS col_min, 
                MAX({col}) AS col_max
            FROM {table}
            WHERE {col} IS NOT NULL;
            '''

        df = pd.read_sql(sql, engine)
        for icol in df.columns:
            profile(table, col, icol, df[icol][0])

#%%
create_profile(engine)

profile_row_counts('census')
profile_row_counts('counties')
profile_col_count('census')
profile_col_count('counties')
profile_col_names('census')
profile_col_names('counties')
profile_numeric_columns('census')
profile_numeric_columns('counties')
#%%

engine.commit()

# %%
print(pd.read_sql('SELECT * FROM profile_summary', engine))

# %%
