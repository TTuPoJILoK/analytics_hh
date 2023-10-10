from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv

# Скрипт загружает  2 csv файла  для инициализации базы данных на сервере
vacs = pd.read_csv('data_new/vacs.csv')
skills = pd.read_csv('data_new/skills.csv')
load_dotenv()
engine = create_engine(os.getenv('POSTGRES_URL_LOCALHOST'))

vacs.to_sql("vacancies", engine, if_exists="append", index=False)
skills.to_sql("skills", engine, if_exists="append", index=False)
