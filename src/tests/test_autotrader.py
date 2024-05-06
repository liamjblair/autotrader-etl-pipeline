import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, ANY
from sqlalchemy import create_engine
from src.autotrader_data_pipeline import AutotraderPipeline
from src import constants


class TestAutotraderPipeline:

    @pytest.fixture
    def pipeline(self):
        db_user = constants.DB_USER
        db_password = constants.DB_PASSWORD
        db_host = constants.DB_HOST
        db_port = constants.DB_PORT
        db_name = constants.DB_NAME
        db_table = constants.DB_TABLE

        return AutotraderPipeline(db_user, db_password, db_host,
                                   db_port, db_name, db_table)
    
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({
            'year': ['2019 (19 reg)', '2020 (20 reg)'],
            'power': ['100 BHP', '120 BHP'],
            'engine_size': ['2.0L', '1.6L']
        })

    def test_extract(self, pipeline, tmpdir):
        test_csv_1 = tmpdir.join("test_file1.csv")
        test_csv_1.write("""
                         year,power,engine_size\n
                         2019 (19 reg),100 BHP,2.0L\n
                         """)
        test_csv_2 = tmpdir.join("test_file2.csv")
        test_csv_2.write("""
                         year,power,engine_size\n
                         2020 (20 reg),120 BHP,1.6L\n
                         """)

        pipeline.transform = MagicMock()

        pipeline.extract(str(tmpdir))

        pipeline.transform.assert_called_once()

    def test_transform(self, pipeline, sample_df):
        pipeline.load = MagicMock()

        pipeline.transform(sample_df)

        pipeline.load.assert_called_once()

    def test_load_to_database(self, pipeline, sample_df):    
        # Mock create_engine and DataFrame.to_sql
        with patch('sqlalchemy.create_engine'), \
            patch('pandas.DataFrame.to_sql') as mock_to_sql:
            
            pipeline.load(sample_df)

            # Assert that df.to_sql was called with the correct arguments
            mock_to_sql.assert_called_once_with(constants.DB_TABLE, ANY, if_exists='append', index=False)

