#############################################################################################################
#    Author:       Ouzayr Khedun
#    Update:       21 May 2024
#    Description:  Generate an excel which shows all the tables and columns missings across the DBs
#############################################################################################################

import pandas as pd
from sqlalchemy import create_engine

# Define database names for clarity in outputs
db_names = {
    'db1': 'DB1',
    'db2': 'DB2',
    'db3': 'DB3'
}

# Database connection strings
connection_strings = {
    'db1' : 'mssql+pyodbc://{usr}:{pwd}LOCALHOST\SQLEXPRESS/{DB}?driver=ODBC+Driver+17+for+SQL+Server',
    'db2' : 'mssql+pyodbc://{usr}:{pwd}LOCALHOST\SQLEXPRESS/{DB}?driver=ODBC+Driver+17+for+SQL+Server',
    'db3' : 'mssql+pyodbc://{usr}:{pwd}LOCALHOST\SQLEXPRESS/{DB}?driver=ODBC+Driver+17+for+SQL+Server'
}

# Create engine instances
engines = {db_key: create_engine(conn_str) for db_key, conn_str in connection_strings.items()}

def fetch_tables(engine):
    """Fetches table names from a given database"""
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'dbo' AND table_type = 'BASE TABLE';
    """
    return pd.read_sql_query(query, engine)

def fetch_columns(table_name, engine):
    """Fetches column names from a given table in a given database"""
    query = f"""
    SELECT column_name
    FROM information_schema.columns 
    WHERE table_name = '{table_name}' AND table_schema = 'dbo';
    """
    try:
        return pd.read_sql_query(query, engine)
    except Exception as e:
        print(f"Failed to fetch columns for table {table_name}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

def compare_databases(engines, db_names):
    # Get tables from all databases
    tables = {db: fetch_tables(engine) for db, engine in engines.items()}
    all_tables = set().union(*(set(df['table_name']) for df in tables.values()))

    # Prepare dataframe to store table presence info
    table_presence = []
    for table in all_tables:
        row = {'TableName': table}
        for db_key, db_name in db_names.items():
            row[db_name] = "Present" if table in set(tables[db_key]['table_name']) else "Not Present"
        table_presence.append(row)

    # Export table presence to CSV
    presence_df = pd.DataFrame(table_presence)
    presence_df.to_csv('table_presence_across_databases.csv', index=False)

    # Prepare column comparison dataframe
    column_presence = []
    for table in all_tables:
        columns_per_db = {db: fetch_columns(table, engine) if table in set(tables[db]['table_name']) else pd.DataFrame() for db, engine in engines.items()}
        all_columns = set().union(*(set(df['column_name'] if 'column_name' in df.columns else []) for df in columns_per_db.values()))

        for column in all_columns:
            row = {'TableName': table, 'ColumnName': column}
            for db_key, db_name in db_names.items():
                row[db_name] = "Present" if not columns_per_db[db_key].empty and column in set(columns_per_db[db_key]['column_name']) else "Not Present"
            column_presence.append(row)

    # Export column differences to CSV
    columns_df = pd.DataFrame(column_presence)
    columns_df.to_csv('column_differences_across_databases.csv', index=False)
    
    return presence_df, columns_df

# Compare the databases
table_presence, column_differences = compare_databases(engines, db_names)
print("Table presence across databases:", table_presence.head())
print("Column differences across databases:", column_differences.head())
