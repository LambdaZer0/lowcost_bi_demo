import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
load_dotenv()

DB_HOST = os.getenv("DB_HOST")   
DB_PORT = os.getenv("DB_PORT")        
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

connection_uri = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_uri)

# reading new file
df = pd.read_csv("/home/aboubakr/workSpace/lowcost_bi_demo/database/data/BDD_TSPDTS.csv", sep=";")

query = """select cause_key, cause_value from dm_lowcostbi.cause_mapping_table;"""

# loading mapping from db into df
try:
    cause_mapping_df = pd.read_sql_query(query, con=engine)
    print("DataFrame successfully loaded from database.")
except Exception as e:
    print(f"An error occured during read request: {e}")

columns_list = list(df.columns[14::])

# updating the df if a new value occure 
for cause in columns_list:
    if cause  not in cause_mapping_df.cause_value.values:
        row_number = cause_mapping_df.shape[0]
        new_row = [f"cause_{row_number}", cause]
        new_row_series = pd.Series(new_row, index=cause_mapping_df.columns)
        cause_mapping_df = pd.concat([cause_mapping_df, new_row_series.to_frame().T],
                                           ignore_index=True)
        print("DataFrame Updated.")

cause_mapping_df['cause'] = cause_mapping_df['cause_value'].str.extract(r'(^\w+)')

# writing new df into db
try:
    cause_mapping_df.to_sql(name="cause_mapping_table", con=engine, if_exists="replace",
                              index=False, schema="dm_lowcostbi")
    print("DataFrame successfully saved into database.")
except Exception as e:
    print(f"An error occured during write request: {e}")