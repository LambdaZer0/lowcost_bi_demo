import pandas as pd
from sqlalchemy import create_engine
DB_HOST = "192.168.1.169"   
DB_PORT = "5432"        
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "hcmK1qiP%251001%21"

connection_uri = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_uri)

# reading new file
df = pd.read_csv("/home/aboubakr/workSpace/lowcost_bi_demo/database/data/BDD_TSPDTS.csv", sep=";")

query = """select cause_key, cause_value from dm_lowcostbi.columns_mapping_table;"""

# loading mapping from db into df
try:
    columns_mapping_table = pd.read_sql_query(query, con=engine)
    print("DataFrame successfully loaded from database.")
except Exception as e:
    print(f"An error occured during read request: {e}")
columns_list = list(df.columns[14::])

# updating the df if a new value occure 
for cause in columns_list:
    if cause  not in columns_mapping_table.cause_value.values:
        row_number = columns_mapping_table.shape[0]
        new_row = [f"cause_{row_number}", cause]
        new_row_series = pd.Series(new_row, index=columns_mapping_table.columns)
        columns_mapping_table = pd.concat([columns_mapping_table, new_row_series.to_frame().T],
                                           ignore_index=True)
        print("DataFrame Updated.")

# writing new df into db
try:
    columns_mapping_table.to_sql(name="columns_mapping_table", con=engine, if_exists="replace",
                              index=False, schema="dm_lowcostbi")
    print("DataFrame successfully saved into database.")
except Exception as e:
    print(f"An error occured during write request: {e}")