import constants
import pandas as pd
from sqlalchemy import create_engine
import os


class AutotraderPipeline:

    def extract(self):
        
        """loop files in dir and pass to transform method"""

        folder = constants.SOURCE_FOLDER_PATH

        try:
            # TODO loop and append to create one df?
            for file_name in os.listdir(folder):
                source_file = os.path.join(folder, file_name)
                df = pd.read_csv(source_file)
                self.transform(df)

        except Exception as e:
            print(e)

    def transform(self, df):
        
        """clean up dataframe ready for db load"""

        #remove spaces from column names
        df.columns = [col.strip() for col in df.columns]
        print(df.dtypes)

        try:
            # remove '2019 (19 reg)' from year column
            df['year'] = df['year'].str.replace(r' \(\d{2} reg\)' ,'', regex=True)
            # remove text from power column and convert to int
            df['power'] = df['power'].str.replace("BHP|PS", "", regex=True)
            df['power'] = df['power'].astype(int)
            # convert engine_size into float
            df["engine_size"] = df["engine_size"].str.replace("L", "")
            df["engine_size"] = df["engine_size"].astype(float)

            print(df.head())

            self.load(df)
        except Exception as e:
            print(f"Following error ocurred performing transformations {e}")

    def load(self, df):

        """load data in target db table"""

        try:
            conn_str = f"postgresql://{constants.DB_USER}:{constants.DB_PASSWORD}@{constants.DB_HOST}:{constants.DB_PORT}/{constants.DB_NAME}"
            db_engine = create_engine(conn_str)

            # Write data to PostgreSQL database
            tbl = "autotrader_data"
            df.to_sql(tbl, db_engine, if_exists='append', index=False)

            db_engine.dispose()
        except Exception as e:
            print(e)

run_pipeline = AutotraderPipeline()
run_pipeline.extract()