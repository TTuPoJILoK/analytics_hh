import pandas as pd
from sqlalchemy import create_engine
import psycopg2


df = pd.read_csv('skills.csv')
engine = create_engine("postgresql://admin:admin@localhost:5432/analytics_hh")
df.to_sql("skill_types", engine, if_exists="append", index=False)