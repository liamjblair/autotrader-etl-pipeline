# AutoTrader ETL Pipeline

## Overview: 

This is a simple project I created for fun to practice ETL processes and routines. Source data is from my Autotrader Car Valuation project. The .csv's are extracted, transfromed and loaded into a dev posgress database.

## Skills/Technologies
Python / ETL / Unit Tests / Logging / PostgreSQL / Pandas / SQLAlchemy

## Installation:
Clone this repository to your local machine.
Install the required Python packages using pip >>
pip install -r requirements.txt

## Configuration
Ensure to configure the following parameters in the constants.py file:

DB_USER: Database username.
DB_PASSWORD: Database password.
DB_HOST: Database host address.
DB_PORT: Database port.
DB_NAME: Database name.
DB_TABLE: Target database table name.
SOURCE_FOLDER_PATH: Path to the folder containing the source CSV files.

 ## Logging
The project utilizes a logging module for error handling and debugging. Logs are recorded in a file named logger.log.

## Contributions:
I am always keen to learn better ways of doing things and enjoy seeing how different people approach problems! Feel free to submit pull requests or open issues if you encounter any problems!
