import pandas as pd
from sqlalchemy import create_engine
import psycopg2


conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="analytics_hh",
        user="admin",
        password="admin"
        )
cur = conn.cursor()

q = "select id_vac, skill from vacancies"

cur.execute(q)
skills = pd.DataFrame(cur.fetchall())
skills.columns=[x.name for x in cur.description]

conn.commit()
cur.close()


gr = skills.groupby('skill')['id_vac'].count().sort_values(ascending=False)
skills_group = pd.DataFrame(gr)
skills_group = skills_group.reset_index()
skills_group['skill_lower'] = skills_group['skill'].apply(str.lower)
skills_group = skills_group.groupby(['skill_lower']).sum().sort_values('id_vac', ascending=False).reset_index()

keywords_programming = [
'sql', 'python', 'r', 'c', 'c#', 'javascript', 'js',  'java', 'scala', 'sas', 'matlab', 
'c++', 'c/c++', 'perl', 'go', 'typescript', 'bash', 'html', 'css', 'php', 'powershell', 'rust', 
'kotlin', 'ruby',  'dart', 'assembly', 'swift', 'vba', 'lua', 'groovy', 'delphi', 'objective-c', 
'haskell', 'elixir', 'julia', 'clojure', 'solidity', 'lisp', 'f#', 'fortran', 'erlang', 'apl', 
'cobol', 'ocaml', 'crystal', 'javascript/typescript', 'golang', 'nosql', 't-sql', 'no-sql',
'visual_basic', 'pascal', 'mongo', 'pl/sql',  'sass', 'vb.net',  
]

keywords_libraries = [
'scikit-learn', 'jupyter', 'theano', 'openCV', 'spark', 'nltk', 'mlpack', 'chainer', 'fann', 'shogun', 
'dlib', 'mxnet', 'node.js', 'vue', 'vue.js', 'keras', 'ember.js', 'jse/jee', 'seaborn', 'pandas', 'selenium',
'matplotlib', 'dplyr', 'tidyr', 'ggplot2', 'plotly', 'numpy', 'hadoop', 'airflow', 'tensorflow', 'pyspark', 'pytorch',
    
]

keywords_analyst_tools = [
'excel', 'tableau',  'word', 'powerpoint', 'looker', 'powerbi', 'outlook', 'jira', 'twilio',  'snowflake', 
'shell', 'linux', 'sas', 'sharepoint', 'ms visio', 'git', 'powerpoints', 'spreadsheets',
 'gdpr', 'spreadsheet', 'alteryx', 'github', 'ssis', 'power_bi', 'spss', 'ssrs', 
'microstrategy',  'cognos', 'dax',  'esquisse', 'rshiny', 'mlr',
'docker', 'linux', 'jira', 'graphql', 'sap', 'node', 'asp.net', 'unix',
'jquery', 'gitlab', 'splunk', 'bitbucket', 'qlik', 'terminal', 'atlassian', 'unix/linux',
'linux/unix', 'ubuntu', 'nuix', 'datarobot', 'microsoft', 'slack', 'bpmn', 'uml'
]

keywords_db = [
    'mongodb', 'postgresql', 'mssql', 'mysql','postgres', 'redis', 'oracle', 'clickhouse', 'cassandra',
    'mariadb', 'sql server', 'dynamodb'
]

keywords_cloud_tools = [
'aws', 'azure', 'gcp', 'snowflake', 'redshift', 'bigquery', 'aurora', 'databricks', 'ibm cloud', 'heroku'
]

skills_group['prog_skills'] = skills_group['skill_lower'].str.split(expand=True).isin(keywords_programming).any(axis=1)
skills_group['lib_skills'] = skills_group['skill_lower'].str.split(expand=True).isin(keywords_libraries).any(axis=1)
skills_group['tool_skills'] = skills_group['skill_lower'].str.split(expand=True).isin(keywords_analyst_tools).any(axis=1)
skills_group['db_skills'] = skills_group['skill_lower'].str.split(expand=True).isin(keywords_db).any(axis=1)

skills_group.loc[skills_group['prog_skills'] == True, 'type'] = 'Языки программирования'
skills_group.loc[skills_group['lib_skills'] == True, 'type'] = 'Библиотеки'
skills_group.loc[skills_group['tool_skills'] == True, 'type'] = 'Инструменты'
skills_group.loc[skills_group['db_skills'] == True, 'type'] = 'Базы данных'

skills_group['type'] = skills_group['type'].fillna('0')
skills_group.loc[skills_group['skill_lower'] == 'oracle bi', 'type'] = 'Инструменты'

skills_group = skills_group[['skill_lower', 'type']]
skills_group = skills_group.rename(columns={'type': 'skill_type'})
skills_group = skills_group.loc[skills_group['skill_lower'] != '0']

cur = conn.cursor()

cur.execute( '''
    DELETE FROM skill_types;
''')      

conn.commit()
cur.close()
conn.close()

engine = create_engine("postgresql://admin:admin@localhost:5432/analytics_hh")
skills_group.to_sql("skill_types", engine, if_exists="append", index=False)