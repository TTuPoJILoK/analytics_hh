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
'visual_basic', 'pascal', 'mongo', 'pl/sql',  'sass', 'vb.net', '1с: предприятие 8','1с: управление торговлей',
'1с: зарплата и управление персоналом', '1с: бухгалтерия', '1с: документооборот',
'1с: управление производственным предприятием', '1с: комплексная автоматизация', '1с программирование', '1c erp', '1c',
'power query', '1c: предприятие', 'oracle pl/sql', '1с: управление холдингом', '1с-битрикс', '1с: зарплата и кадры',
    '1c: финансы', 'sql запросы'
    
]

keywords_libraries = [
'scikit-learn', 'jupyter', 'theano', 'openCV', 'spark', 'nltk', 'mlpack', 'chainer', 'fann', 'shogun', 
'dlib', 'mxnet', 'node.js', 'vue', 'vue.js', 'keras', 'ember.js', 'jse/jee', 'seaborn', 'pandas', 'selenium',
'matplotlib', 'dplyr', 'tidyr', 'ggplot2', 'plotly', 'numpy', 'hadoop', 'airflow', 'tensorflow', 'pyspark', 'pytorch',
    
]

keywords_analyst_tools = [
'ms excel', 'tableau',  'word', 'ms powerpoint', 'looker', 'power bi', 'outlook', 'jira', 'twilio',  'snowflake', 
'shell', 'linux', 'sas', 'sharepoint', 'ms visio', 'git', 'powerpoints', 'spreadsheets',
 'gdpr', 'spreadsheet', 'alteryx', 'github', 'ssis', 'power_bi', 'spss', 'ssrs', 
'microstrategy',  'cognos', 'dax',  'esquisse', 'rshiny', 'mlr',
'docker', 'linux', 'jira', 'graphql', 'sap', 'node', 'asp.net', 'unix',
'jquery', 'gitlab', 'splunk', 'bitbucket', 'qlik', 'terminal', 'atlassian', 'unix/linux',
'linux/unix', 'ubuntu', 'nuix', 'datarobot', 'microsoft', 'slack', 'bpmn', 'uml', 'rest', 'atlassian jira',
'api', 'erp', 'json api', 'erp-системы на базе 1с', 'ms office', 'trello', 'crm', 'etl', 'a/b тесты', 'kafka',
'ms access', 'confluence', 'google analytics', 'idef0', 'rest api', 'ms power bi', 'idef', 'ms project',
'ms outlook', 'postman', 'битрикс24', 'rabbitmq', 'swagger', 'google docs', 'kanban', 'google таблицы', 'ms word',
'excel', 'powerpoint', 'soap', 'xml', 'scrum', 'xsd', 'sap', ' яндекс метрика', 'figma', 'power pivot',
'superset', 'readmine', 'работа в excel', 'visio', 'oracle bi', 'camunda', 'bitrix24',
'продвинутый пользователь ms excel', 'itsm', 'datalens', 'google sheets', 'знание excel', 'mc excel',
'bitrix', 'grafana', 'apache kafka'
]

keywords_db = [
    'mongodb', 'postgresql', 'ms sql', 'mysql','postgres', 'redis', 'oracle', 'clickhouse', 'cassandra',
    'mariadb', 'sql server', 'dynamodb', 'ms sql server', 'greenplum'
]


skills_group['prog_skills'] = skills_group['skill_lower'].isin(keywords_programming)
skills_group['lib_skills'] = skills_group['skill_lower'].isin(keywords_libraries)
skills_group['tool_skills'] = skills_group['skill_lower'].isin(keywords_analyst_tools)
skills_group['db_skills'] = skills_group['skill_lower'].isin(keywords_db)

skills_group.loc[skills_group['prog_skills'] == True, 'type'] = 'Языки программирования'
skills_group.loc[skills_group['lib_skills'] == True, 'type'] = 'Библиотеки'
skills_group.loc[skills_group['tool_skills'] == True, 'type'] = 'Инструменты'
skills_group.loc[skills_group['db_skills'] == True, 'type'] = 'Базы данных'

skills_group['type'] = skills_group['type'].fillna('0')
skills_group = skills_group[['skill_lower', 'id_vac', 'type']]
skills_group = skills_group.rename(columns={'id_vac': 'count', 'type': 'skill_type'})

keywords_programming_fullnames = {
    'sql' : 'SQL', 'python' : 'Python', 'r' : 'R', 'c':'C', 'c#':'C#', 'javascript' : 'JavaScript', 'js':'JS',
    'java':'Java', 
    'scala':'Scala', 'sas' : 'SAS', 'matlab': 'MATLAB', 'c++' : 'C++', 'c/c++' : 'C / C++', 'perl' : 'Perl',
    'go' : 'Go',
    'typescript' : 'TypeScript','bash':'Bash','html' : 'HTML','css' : 'CSS','php' : 'PHP','powershell' : 'Powershell',
    'rust' : 'Rust', 'kotlin' : 'Kotlin','ruby' : 'Ruby','dart' : 'Dart','assembly' :'Assembly',
    'swift' : 'Swift','vba' : 'VBA','lua' : 'Lua','groovy' : 'Groovy','delphi' : 'Delphi',
    'objective-c' : 'Objective-C',
    'haskell' : 'Haskell','elixir' : 'Elixir','julia' : 'Julia','clojure': 'Clojure','solidity' : 'Solidity',
    'lisp' : 'Lisp','f#':'F#','fortran' : 'Fortran','erlang' : 'Erlang','apl' : 'APL','cobol' : 'COBOL',
    'ocaml': 'OCaml','crystal':'Crystal','javascript/typescript' : 'JavaScript / TypeScript','golang':'Golang',
    'nosql': 'NoSQL', 'mongodb' : 'MongoDB','t-sql' :'Transact-SQL', 'no-sql' : 'No-SQL',
    'visual_basic' : 'Visual Basic',
    'pascal':'Pascal', 'mongo' : 'Mongo', 'pl/sql' : 'PL/SQL','sass' :'Sass', 'vb.net' : 'VB.NET','mssql' : 'MSSQL',
    '1с: предприятие 8': '1С: Предприятие 8','1с: управление торговлей': '1C: Управление торговлей',
    '1с: зарплата и управление персоналом': '1C: Зарплата и управление персоналом',
    '1с: бухгалтерия': '1C: Бухгалтерия',
    '1с: документооборот': '1C: Документооборот', '1с: управление производственным предприятием': 
    '1C: Управление производственным предприятием', '1с: комплексная автоматизация': '1C: Комплексная автоматизация',
    '1с программирование': '1C: Программирование', '1c erp': '1C: ERP', '1c': '1C', 'power query': 'Power Query',
    '1c: предприятие': '1C: Предприятие', 'oracle pl/sql': 'Oracle PL/SQL', '1с: управление холдингом':
    '1C: Управление холдингом', '1с-битрикс': '1C-Битрикс', '1с: зарплата и кадры': '1C: Зарплата и кадры',
    '1c: финансы': '1C: Финансы', 'sql запросы': 'SQL'
}

keywords_libraries_fullnames = {
    'scikit-learn': 'Scikit Learn', 'jupyter': 'Jupyter', 'theano': 'Theano', 'opencv': 'openCV',
    'spark': 'Spark', 'nltk': 'NLTK', 'mlpack': 'mlpack', 'chainer': 'Chainer', 'fann': 'Fann',
    'shogun': 'Shogun','node': 'Node.js',  'dlib': 'Dlib', 'mxnet': 'MXNet', 'node.js': 'Node.js',
    'vue': 'Vue.js', 'vue.js': 'Vue.js', 'keras': 'Keras', 'ember.js': 'Ember.js',
    'jse/jee': 'JSE/JEE', 'seaborn': 'Seaborn', 'pandas': 'Pandas', 'selenium': 'Selenium',
    'matplotlib': 'Matplotlib', 'dplyr': 'dplyr', 'tidyr': 'tidyr', 'ggplot2': 'ggplot2', 'plotly': 'Plotly',
    'numpy': 'NumPy', 'hadoop': 'Hadoop', 'airflow': 'Airflow', 'tensorflow': 'TensorFlow', 
    'pyspark': 'PySpark', 'pytorch': 'PyTorch'
}

keywords_analyst_tools_fullnames = {
    'ms excel': 'MS Excel', 'tableau': 'Tableau',  'word': 'MS Word', 'ms powerpoint': 'MS Powerpoint',
    'looker': 'Looker', 'power bi': 'Power BI', 'outlook': 'MS Outook', 'jira': 'Jira', 'twilio': 'Twilio',
    'snowflake': 'Snowflake', 'shell': 'Shell', 'linux': 'Linux', 'sas': 'SAS', 'sharepoint': 'Sharepoint',
    'ms visio': 'MS Visio', 'git': 'Git', 'powerpoints': 'MS Powerpoint', 'spreadsheets': 'Google Sheets',
    'gdpr': 'GDPR', 'spreadsheet': 'Google Sheets', 'alteryx': 'Alteryx', 'github': 'Git', 'ssis': 'SSIS', 
    'power_bi': 'Power BI', 'spss': 'SPSS', 'ssrs': 'SSRS', 'microstrategy': 'MicroStrategy',  
    'cognos': 'Cognos', 'dax': 'DAX',  'esquisse': 'Esquisse', 'docker': 'Docker', 'graphql': 'GraphQL',
    'sap': 'SAP', 'asp.net': 'ASP.NET', 'unix': 'Unix', 'jquery': 'jQuery',
    'gitlab': 'GitLab', 'splunk': 'Splunk', 'bitbucket': 'Bitbucket', 'qlik': 'Qlik', 'terminal': 'Terminal',
    'atlassian': 'Atlassian', 'unix/linux': 'Unix/Linux', 'linux/unix': 'Linux/Unix', 'ubuntu': 'Ubuntu',
    'nuix': 'Nuix', 'datarobot': 'DataRobot', 'microsoft': 'Microsoft', 'slack': 'Slack', 'bpmn': 'BPMN',
    'uml': 'UML', 'rest': 'REST', 'atlassian jira': 'Jira', 'api': 'API', 'erp': 'ERP',
    'json api': 'JSON API', 'erp-системы на базе 1с': 'ERP', 'ms office': 'MS Office', 'trello': 'Trello',
    'crm': 'CRM', 'etl': 'ETL', 'a/b тесты': 'A/B Тесты', 'kafka': 'Kafka', 'ms access': 'MS Access',
    'confluence': 'Confluence', 'google analytics': 'Google Analytics', 'idef0': 'IDEF0', 'rest api': 'REST',
    'ms power bi': 'Power BI', 'idef': 'IDEF', 'ms project': 'MS Project',
    'ms outlook': 'MS Outlook', 'postman': 'Postman', 'битрикс24': 'Битрикс24', 'rabbitmq': 'RabbirMQ',
    'swagger': 'Swagger', 'google docs': 'Google Docs', 'kanban': 'Kanban', 'google таблицы': 'Google Sheets',
    'ms word': 'MS Word', 'excel': 'MS Excel', 'powerpoint': 'MS Powerpoint', 'soap': 'SOAP', 
    'xml': 'XML', 'scrum': 'SCRUM', 'xsd': 'XSD', 'sap': 'SAP', ' яндекс метрика': 'Яндекс.Метрика',
    'figma': 'Figma', 'power pivot': 'Power Pivot', 'superset': 'Superset', 'readmine': 'Readmine',
    'работа в excel': 'MS Excel', 'visio': 'MS Visio', 'oracle bi': 'Oracle BI', 'camunda': 'Camunda',
    'bitrix24': 'Битрикс24', 'продвинутый пользователь ms excel': 'MS Excel', 'itsm': 'ITSM',
    'datalens': 'DataLens', 'google sheets': 'Google Sheets', 'знание excel': 'MS Excel', 'mc excel': 'MS Excel',
    'bitrix': 'Битрикс24', 'grafana': 'Grafana', 'apache kafka': 'Kafka'
}

keywords_db_fullnames = {
    'mongodb': 'MongoDB', 'postgresql': 'PostrgreSQL', 'ms sql': 'MS SQL Server', 'mysql': 'MySQL',
    'postgres': 'PostrgreSQL', 'redis': 'Redis', 'oracle': 'Oracle', 'clickhouse': 'Clickhouse' ,
    'cassandra': 'Cassandra', 'mariadb': 'MariaDB', 'sql server' : 'MS SQL Server', 
    'dynamodb': 'DynamoDB', 'ms sql server': 'MS SQL Server', 'greenplum': 'Greenplum'
    }

keywords_fullnames = {}
keywords_fullnames.update(keywords_programming_fullnames)
keywords_fullnames.update(keywords_libraries_fullnames)
keywords_fullnames.update(keywords_analyst_tools_fullnames)
keywords_fullnames.update(keywords_db_fullnames)

skills_itog = skills_group[skills_group['skill_type'] != '0']
skills_itog['skill_right_name'] = skills_itog['skill_lower'].apply(lambda x: keywords_fullnames[x])
skills_itog = skills_itog[['skill_lower', 'skill_type', 'skill_right_name']]

cur = conn.cursor()

cur.execute( '''
    DELETE FROM skills;   
''')      

conn.commit()
cur.close()
conn.close()

engine = create_engine("postgresql://admin:admin@localhost:5432/analytics_hh")
skills_itog.to_sql("skills", engine, if_exists="append", index=False)