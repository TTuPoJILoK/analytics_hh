import requests
import time
import pandas as pd
import json
from datetime import datetime, timedelta
import fake_useragent
from random import randint
from sqlalchemy import create_engine
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum


local_tz = pendulum.timezone("Asia/Krasnoyarsk")

default_args = {
    'owner': 'oleg',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 29, tzinfo=local_tz)
}

shedule_interval = '0 11 * * *'

# Функция для получения страницы с вакансиями
def getPage(day_from, day_to, page=0):

        # Справочник для параметров GET-запроса
    params = {
        'text': "NAME:Аналитик",  # Вакансии по запросу "Аналитик"
        'area': 113,              # Россия
        'date_from': day_from,    
        'date_to': day_to,        
        'page': page,
        'per_page': 100,          # Кол-во вакансий на 1 странице
    }
    ua = fake_useragent.UserAgent()
    url = "https://api.hh.ru/vacancies"
    req = ''
    while req == '':
        try:
            req = requests.get(url, params, headers={"user-agent":ua.random})
            break
        except:
            time.sleep(5)
            continue
    # Декодируем его ответ, чтобы Кириллица отображалась корректно
    data = req.content.decode()
    req.close()
    return data

#  Функция для получения навыков и описания для конкретной вакансии
def get_skills(vac):
    url = f"https://api.hh.ru/vacancies/{str(vac)}"
    ua = fake_useragent.UserAgent()
    req = requests.get(url, headers={"user-agent":ua.random})
    if req.status_code != 200:
        raise Exception
    time.sleep(randint(1, 3))
    data = req.content.decode()
    js = json.loads(data)
    skills = []
    if 'key_skills' in js and len(js['key_skills']) > 0:
        for skill in js['key_skills']:
            skills.append(skill['name'])
    else:
        skills = '0'
    if 'description' in js:
        description = js['description']
    else:
        description = '0'
    return skills, description

 # Проверка вакансии на то, что она уже была в базе данных
def test_include(vac_id):
    hook = PostgresHook(postgres_conn_id="postgres_con")
    df = hook.get_pandas_df(sql="select distinct id_vac from vacancies;")

    id_arr = list(df['id_vac'])

    if vac_id in id_arr:
        return 1
    else:
        return 0


@dag(default_args=default_args, schedule_interval=shedule_interval, catchup=False)
def dag_parser():

    @task()
    def extract():
        jsobjs = []
        num_parsing_days = 1 # Число дней за которые производим парсинг
        day_from = datetime.now(tz=local_tz) - timedelta(days=num_parsing_days)
        day_to = datetime.now(tz=local_tz) - timedelta(days=num_parsing_days)

        # Парсим частями по четверти дня
        for _ in range(num_parsing_days * 4 - 1):
            day_to += timedelta(days=1/4)
            for page in range(0, 20):
                
                # Преобразуем текст ответа запроса в словарь Python
                
                jsObj = json.loads(getPage(day_from.isoformat(), day_to.isoformat(), page))
                
                jsobjs.extend(jsObj['items'])
                
                # Проверка на последнюю страницу, если вакансий меньше 2000
                if (jsObj['pages'] - page) <= 1:
                    break
                
                # Необязательная задержка, но чтобы не нагружать сервисы hh
                time.sleep(3)
                
            day_from += timedelta(days=1/4)

        jobs = pd.DataFrame(jsobjs)
        return jobs

    @task()
    def transform(jobs):
        # Преобразуем поля в нашей таблице
        jobs = jobs.copy(deep=True)
        jobs['city'] = jobs['area'].apply(lambda x: x['name'] if x != None else 0)
        jobs['employer'] = jobs['employer'].apply(lambda x: x['name'] if x != None else 0)
        jobs['type'] = jobs['type'].apply(lambda x: x['id'] if x != None else 0)
        jobs['experience'] = jobs['experience'].apply(lambda x: x['id'] if x != None else 0)
        jobs['employment'] = jobs['employment'].apply(lambda x: x['id'] if x != None else 0)
        jobs['roles'] = jobs['professional_roles'].apply(lambda a: [x['name'] for x in a])
        jobs['role'] = jobs['roles'].apply(lambda x: x[0])
        jobs = jobs[['id', 'published_at', 'name', 'city', 'salary', 'employer', 'type', 'experience', 'employment', 'role']]

        currencies = {}
        dictionaries = requests.get('https://api.hh.ru/dictionaries').json()
        for currency in dictionaries['currency']:
            currencies[currency['code']] = currency['rate'] 
        currencies[0] = 1

        jobs_salary = pd.json_normalize(jobs['salary'])
        jobs_salary = jobs_salary.fillna(0)

        jobs_salary.loc[(jobs_salary['from'] == 0) & (
            jobs_salary['to'] == 0), 'itog'] = 0
        jobs_salary.loc[(jobs_salary['from'] != 0) & (jobs_salary['to'] != 0), 'itog'] = (
            jobs_salary['from'] + jobs_salary['to']) / 2
        jobs_salary.loc[(jobs_salary['from'] == 0) & (
            jobs_salary['to'] != 0), 'itog'] = jobs_salary['to']
        jobs_salary.loc[(jobs_salary['from'] != 0) & (
            jobs_salary['to'] == 0), 'itog'] = jobs_salary['from']

        jobs_salary['rate'] = jobs_salary.currency.apply(lambda x: currencies[x])
        jobs_salary['itog'] /= jobs_salary['rate']

        jobs_salary.loc[(jobs_salary['gross'] == True), 'itog'] = jobs_salary['itog'] * 0.87

        jobs = jobs.join(jobs_salary, lsuffix='_caller', rsuffix='_other')
        jobs = jobs[['id', 'published_at', 'name', 'city', 'itog', 'employer',
                    'type', 'experience', 'employment', 'role']]
        jobs = jobs.rename(columns={'itog': 'salary'})

        roles = ['Системный аналитик', 'Продуктовый аналитик', 'BI-аналитик, аналитик данных', 'Бизнес-аналитик', 
                'Аналитик', 'Руководитель отдела аналитики', 'Финансовый аналитик, инвестиционный аналитик']
        jobs = jobs[jobs['role'].isin(roles)]

        jobs.loc[jobs['experience'] == 'between1And3', 'experience'] = 'От 1 до 3 лет'
        jobs.loc[jobs['experience'] == 'between3And6', 'experience'] = 'От 3 до 6 лет'
        jobs.loc[jobs['experience'] == 'moreThan6', 'experience'] = 'Более 6 лет'
        jobs.loc[jobs['experience'] == 'noExperience', 'experience'] = 'Без опыта'

        jobs.loc[jobs['role'] == 'BI-аналитик, аналитик данных', 'role'] = 'BI-аналитик'
        jobs.loc[jobs['experience'] == 'От 1 до 3 лет', 'grade'] = 'Middle ' + jobs['role']
        jobs.loc[jobs['experience'] == 'От 3 до 6 лет', 'grade'] = 'Senior ' + jobs['role']
        jobs.loc[jobs['experience'] == 'Более 6 лет', 'grade'] = 'Senior ' + jobs['role']
        jobs.loc[jobs['experience'] == 'Без опыта', 'grade'] = 'Middle ' + jobs['role']
        jobs.loc[jobs['role'] == 'Руководитель отдела аналитики', 'grade'] = 'Руководитель отдела аналитики'

        jobs = jobs.drop_duplicates(subset='id', ignore_index=True)

        jobs['drop'] = jobs['id'].apply(test_include)

        jobs_res = jobs[jobs['drop'] == 0].copy()

        # К каждой вакансии добавляяем навыки и описания
        vacs = jobs_res['id'].to_list()
        i = 0
        skills = {}
        descriptions = {}

        # Обрабатываем вакансии по 100, чтобы не нагружать сервисы hh.ru
        while i < len(vacs):
            batch = vacs[i:i+100]
            for vac in batch:
                skills[vac], descriptions[vac] = get_skills(vac)
            time.sleep(randint(5, 8))
            i += 100

        jobs_res['description'] = jobs_res['id'].map(descriptions)
        jobs_res['skill'] = jobs_res['id'].map(skills)
        jobs_res = jobs_res.explode('skill')
        jobs_res = jobs_res.reset_index()
        jobs_res = jobs_res[['id', 'published_at', 'name', 'city', 'salary', 'employer', 'type', 'experience', 
                            'employment', 'role', 'description', 'skill', 'grade']]

        # Из таблицы skill_types добавляем типы навыков
        hook = PostgresHook(postgres_conn_id="postgres_con")
        skills = hook.get_pandas_df(sql="select * from skills;")

        skills = skills[['skill_lower', 'skill_type', 'skill_right_name']]
        jobs_res['skill_lower'] = jobs_res['skill'].apply(str.lower)
        jobs_res = jobs_res.merge(skills, on='skill_lower', how='left')
        jobs_res = jobs_res.rename(columns={'id': 'id_vac', 'type': 'type_vac'})
        jobs_res = jobs_res[['id_vac', 'published_at', 'name', 'city', 'salary', 'employer', 'type_vac', 'experience', 
                         'employment', 'role', 'description', 'skill', 'grade', 'skill_lower', 'skill_type',
                    'skill_right_name']]
        jobs_res['skill_type'] = jobs_res['skill_type'].fillna('0')
        jobs_res['skill_right_name'] = jobs_res['skill_right_name'].fillna('0')

        #  Добавляем в таблицу primary key
        max_id_df = hook.get_pandas_df(sql="SELECT MAX(column_not_exist_in_db) as max FROM vacancies;")
        max_id = max_id_df['max'][0] + 1
        jobs_res['column_not_exist_in_db'] = range(max_id, max_id+len(jobs_res))
        return jobs_res

    @task
    def load(jobs_res):
        # Складываем результат в Postgres
        engine = create_engine("postgresql://admin:admin@app_db:5432/analytics_hh")
        jobs_res.to_sql('vacancies', engine, if_exists='append', index=False)

    jobs = extract()
    jobs_res = transform(jobs)
    load(jobs_res)

dag_parser = dag_parser()
