import requests
import time
import pandas as pd
import json
from datetime import datetime, timedelta
import fake_useragent
from random import randint
from sqlalchemy import create_engine
import psycopg2


# Метод получения страницы с вакансиями
# Аргументы: page - Индекс страницы, начинается с 0. day_from, day_to - период, за который парсим вакансии
def getPage(day_from, day_to, page=0):

    # Справочник для параметров GET-запроса
    params = {
        'text': "NAME:Аналитик",  # Вакансии по запросу "Аналитик"
        'area': 113,              # Россия
        'date_from': day_from,
        'date_to': day_to,
        'page': page,
        'per_page': 100,  # Кол-во вакансий на 1 странице
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

# Функция получения навыков и описания конкретной вакансии
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

def test_include(vac_id):
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="analytics_hh",
        user="admin",
        password="admin"
        )
    cur = conn.cursor()

    q = "select distinct id_vac from vacancies"

    cur.execute(q)
    id_arr = cur.fetchall()
    id_arr = [x[0] for x in id_arr]

    conn.commit()
    cur.close()
    conn.close()

    if vac_id in id_arr:
        return 1
    else:
        return 0


jsobjs = []
day_from = datetime.now() - timedelta(days=5)
day_to = datetime.now() - timedelta(days=5)


#########################################
print('start')
#########################################


for _ in range(19):
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

################
print('jobs parsing done')
###############

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

################
print('jobs correcting done')
###############

jobs['drop'] = jobs['id'].apply(test_include)

jobs_res = jobs[jobs['drop'] == 0].copy()

################
print('jobs delete doubles from db done')
###############

vacs = jobs_res['id'].to_list()
i = 0
skills = {}
descriptions = {}

while i < len(vacs):
    batch = vacs[i:i+100]
    for vac in batch:
        skills[vac], descriptions[vac] = get_skills(vac)
    time.sleep(randint(5, 8))
    i += 100

################
print('skills descroptions done')
###############

jobs_res['description'] = jobs_res['id'].map(descriptions)
jobs_res['skill'] = jobs_res['id'].map(skills)
jobs_res = jobs_res.explode('skill')
jobs_res = jobs_res.reset_index()
jobs_res = jobs_res[['id', 'published_at', 'name', 'city', 'salary', 'employer', 'type', 'experience', 
                     'employment', 'role', 'description', 'skill', 'grade']]

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="analytics_hh",
    user="admin",
    password="admin"
)
cur = conn.cursor()
cur.execute( '''
    SELECT *
    FROM skill_types
''')
            
skills = pd.DataFrame(cur.fetchall())
skills.columns=[ x.name for x in cur.description]
conn.commit()
cur.close()
conn.close()

jobs_res['skill_lower'] = jobs_res['skill'].apply(str.lower)

jobs_res = jobs_res.merge(skills, on='skill_lower')
jobs_res = jobs_res.rename(columns={'id': 'id_vac', 'type_x': 'type_vac', 'type_y': 'skill_type'})
jobs_res = jobs_res = jobs_res[['id_vac', 'published_at', 'name', 'city', 'salary', 'employer', 'type', 'experience', 
                     'employment', 'role', 'description', 'skill', 'grade', 'skill_lower', 'skill_type']]

################
print('jobs all done')
###############

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="analytics_hh",
    user="admin",
    password="admin"
)
cur = conn.cursor()
cur.execute( '''
    SELECT MAX(column_not_exist_in_db)
    FROM vacancies
''')
            
max_id = cur.fetchall()[0][0] + 1
max_id

jobs_res['column_not_exist_in_db'] = range(max_id, max_id+len(jobs_res))

engine = create_engine("postgresql://admin:admin@localhost:5432/analytics_hh")
jobs_res.to_sql("vacancies", engine, if_exists="append", index=False)

################
print('the end')
###############