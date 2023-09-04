from sqlalchemy import create_engine
import pandas as pd

# Скрипт загружает  2 csv файла  для инициализации базы данных на сервере
vacs = pd.read_csv('data/vacs.csv')
skills = pd.read_csv('data/skills.csv')
engine = create_engine("postgresql://admin:admin@localhost:5432/analytics_hh")

vacs.to_sql("vacancies", engine, if_exists="append", index=False)
skills.to_sql("skill_types", engine, if_exists="append", index=False)