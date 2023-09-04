from contextlib import contextmanager
from datetime import datetime
from typing import List
from typing import Optional
import datetime

import pytz as pytz
from sqlalchemy import ForeignKey, create_engine
from sqlalchemy import String, FLOAT, DateTime, Integer
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy_utils import database_exists, create_database


class Base(DeclarativeBase):
    pass

# Создание таблиц и базы данных на сервере
class Vacancy(Base):
    __tablename__ = "vacancies"

    column_not_exist_in_db = mapped_column(Integer, primary_key=True)
    id_vac: Mapped[str] = mapped_column(String)
    published_at: Mapped[datetime.datetime] = mapped_column(DateTime)
    name: Mapped[str] = mapped_column(String)
    city: Mapped[str] = mapped_column(String)
    salary: Mapped[float] = mapped_column(FLOAT)
    employer: Mapped[str] = mapped_column(String)
    type_vac: Mapped[str] = mapped_column(String)
    experience: Mapped[str] = mapped_column(String)
    employment: Mapped[str] = mapped_column(String)
    role: Mapped[str] = mapped_column(String)
    description: Mapped[str] = mapped_column(String)
    skill: Mapped[str] = mapped_column(String)
    skill_lower: Mapped[str] = mapped_column(String)
    skill_type: Mapped[str] = mapped_column(String)
    grade: Mapped[str] = mapped_column(String)



class Skills(Base):
    __tablename__ = "skill_types"

    column_not_exist_in_db = mapped_column(Integer, primary_key=True)
    skill_lower: Mapped[str] = mapped_column(String)
    skill_type: Mapped[str] = mapped_column(String)


#postgresql://admin:admin@localhost:5432/analytics_hh
engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
if not database_exists(engine.url):
    create_database(engine.url)
    
Base.metadata.create_all(engine)


# Dependency
def get_db():
    db = Session(engine)
    try:
        yield db
    finally:
        db.close()
