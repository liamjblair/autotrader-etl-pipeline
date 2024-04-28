import constants
import pandas as pd
from sqlalchemy import create_engine
import os
import logger as logging


class AutotraderPipeline:

    def __init__(self, db_user, db_password, db_host, db_port, db_name, db_table):
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_table = db_table

    def extract(self, source_file_path):
        
        """loop source files in dir (.csv) then pass to transform method"""

        try:
            # TODO loop and append to create one df?
            for file_name in os.listdir(source_file_path):
                source_file = os.path.join(source_file_path, file_name)
                df = pd.read_csv(source_file)
                self.transform(df)

        except Exception as e:
            logging.logger.error(f"Following error ocurring accessing source files - {e}")

    def transform(self, df):
        
        """clean up dataframe ready for db load"""

        #remove spaces from column names
        df.columns = [col.strip() for col in df.columns]
        print(df.dtypes)

        try:
            # remove '2019 (19 reg)' from year column
            df['year'] = df['year'].str.replace(r" \(\d{2} reg\)" ,"", regex=True)
            # remove text from power column and convert to int
            df['power'] = df['power'].str.replace("BHP|PS", "", regex=True)
            df['power'] = df['power'].astype(int)
            # convert engine_size into float
            df["engine_size"] = df["engine_size"].str.replace("L", "")
            df["engine_size"] = df["engine_size"].astype(float)

            print(df.head())

            self.load(df)
        except Exception as e:
            logging.logger.error(f"Following error ocurred performing transformations {e}")

    def load(self, df):

        """load data in target db table"""

        try:
            conn_str = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
            db_engine = create_engine(conn_str)

            # Write data to PostgreSQL database
            df.to_sql(self.db_table, db_engine, if_exists='append', index=False)

            db_engine.dispose()
        except Exception as e:
            logging.logger.error(f"Following error ocurring attemping to load data in to posgres db = {e}")

db_user = constants.DB_USER
db_password = constants.DB_PASSWORD
db_host = constants.DB_HOST
db_port = constants.DB_PORT
db_name = constants.DB_NAME
db_table = constants.DB_TABLE
source_folder = constants.SOURCE_FOLDER_PATH

run_pipeline = AutotraderPipeline(db_user, 
                                  db_password, 
                                  db_host, 
                                  db_port, 
                                  db_name,
                                  db_table)
run_pipeline.extract(source_folder)