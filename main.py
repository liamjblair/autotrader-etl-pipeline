from src.autotrader_data_pipeline import AutotraderPipeline
from src import constants

def main():
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

if __name__ == "__main__":
    main()