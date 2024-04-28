import dotenv
import os

dotenv.load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_TABLE = os.getenv("DB_TABLE")

SOURCE_FOLDER_PATH = os.getenv("SOURCE_FOLDER_PATH")