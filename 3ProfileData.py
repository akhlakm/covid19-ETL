#%%
import os
import sqlite3
import pandas as pd

profiler_version = 0.1
engine = sqlite3.connect('covid19.sqlite.db')

#%% Schema
schema = ['table', 'column', 'info', 'value', 'profilev']
df = pd.DataFrame(columns = schema)
df = df.astype({'profilev': str})
try:
    df.to_sql('profile_summary', con=engine, if_exists='replace', index=False)
except ValueError:
    pass

#%% Profile - Counts

def profile_row_counts(tablename):
    info = 'rows'
    column = 'all'

    sql = f"""
    INSERT INTO profile_summary
    ('table', 'column', 'info', 'value', 'profilev')
    VALUES ('{tablename}', '{column}', '{info}', (
        SELECT COUNT(*) FROM {tablename}
    ), DATE('now'));
    """
    engine.execute(sql)

def col_names(tablename):
    return pd.read_sql(f'SELECT * FROM {tablename} LIMIT 0', engine).columns.values


def profile_col_names(tablename):
    value = ",".join(col_names(tablename))

    info = 'colnames'
    column = 'all'

    sql = f"""
    INSERT INTO profile_summary
    ('table', 'column', 'info', 'value', 'profilev')
    VALUES ('{tablename}', '{column}', '{info}', '{value}', DATE('now'));
    """
    print(sql)
    engine.execute(sql)

def profile_col_count(tablename):
    value = len(col_names(tablename))

    info = 'colcount'
    column = 'all'

    sql = f"""
    INSERT INTO profile_summary
    ('table', 'column', 'info', 'value', 'profilev')
    VALUES ('{tablename}', '{column}', '{info}', "{value}", DATE('now'));
    """
    print(sql)
    engine.execute(sql)


#%%
profile_row_counts('census')
profile_row_counts('counties')
profile_col_count('census')
profile_col_count('counties')
profile_col_names('census')
profile_col_names('counties')

# %%
print(pd.read_sql('SELECT * FROM profile_summary', engine))

# %%
