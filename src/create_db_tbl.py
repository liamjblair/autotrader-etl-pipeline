
import psycopg2
import constants
import pandas as pd

# Establish connection to the PostgreSQL database
conn = psycopg2.connect(
    dbname=constants.DB_NAME,
    user=constants.DB_USER,
    password=constants.DB_PASSWORD,
    host=constants.DB_HOST,
    port=constants.DB_PORT
)

cur = conn.cursor()

# CREATE TABLE SQL command
create_table_query = """
    CREATE TABLE IF NOT EXISTS postgres.public.autotrader_data (
        id SERIAL PRIMARY KEY,
        price int,
        year varchar(20),
        mileage int,
        engine_size varchar(10),
        power varchar(10),
        transmission varchar(25),
        fueltype varchar(50),
        url varchar(255)
    );
"""

cur.execute(create_table_query)

# Commit and close the transaction
conn.commit()
cur.close()
conn.close()
