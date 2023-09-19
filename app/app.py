import pandas as pd
import streamlit as st
import plotly.express as px
import psycopg2
from datetime import date, timedelta, datetime
import numpy as np
from plotly.colors import n_colors

# Выгружаем данные
conn = psycopg2.connect(
    host="app_db", 
    port="5432",
    database="analytics_hh",
    user="admin",
    password="admin"
)
cur = conn.cursor()
cur.execute( '''
    SELECT id_vac, 
            published_at, 
            name, 
            city, 
            salary, 
            employer, 
            type_vac, 
            experience, 
            employment, 
            role, 
            skill, 
            skill_right_name,
            skill_type,
            grade
    FROM vacancies
''')     
vacs = pd.DataFrame(cur.fetchall())
vacs.columns=[ x.name for x in cur.description]
conn.commit()
cur.close()
conn.close()       

st.set_page_config(page_title='Analytics',
                   page_icon=':bar_chart:',
                   layout="wide")

st.header('Анализ рынка вакансий аналитиков на HeadHunter')

########### Фильтры ##############
roles = list(vacs['role'].unique())
roles.sort()
roles.insert(0, 'Все вакансии')

cities = list(vacs['city'].unique())
cities.sort()
cities.insert(0, 'Все города')

exp = list(vacs['experience'].unique())
exp.sort()
exp.insert(0, 'Любой опыт')

dates = pd.to_datetime(vacs['published_at']).dt.date
start_date = dates.min()
end_date = dates.max()

st.sidebar.header("Фильтры по навыкам:")
role_selection = st.sidebar.multiselect(
    'Вакансия:',
    options=roles,
    default = 'Все вакансии')

if "Все вакансии" in role_selection:
    role_selection = roles


exp_selection = st.sidebar.multiselect(
    'Опыт работы:',
     options=exp, 
     default='Любой опыт')

if "Любой опыт" in exp_selection:
    exp_selection = exp

selection_date = st.sidebar.slider(
    'Период:',
    min_value=start_date,
    max_value=end_date,
    value=(start_date, end_date),
    step=timedelta(1),
)

st.sidebar.header("Общие фильтры:")
city_selection = st.sidebar.multiselect(
    'Город:',
     options=cities,
     default='Все города')

if "Все города" in city_selection:
    city_selection = cities

num_results = st.sidebar.radio('Количетсво:', ('Топ 10', 'Топ 20'))

mask = vacs['published_at'].dt.date.between(*selection_date)
vacs2 = vacs[mask]

selection = vacs2.query(
    "experience == @exp_selection & role == @role_selection & city == @city_selection"
)

try:
    # Строим KPI
    num_vacs = selection['id_vac'].nunique()
    avg_salary = round(selection[selection['salary'] != 0]['salary'].mean())

    left_coloumn, middle_column, right_column = st.columns(3)

    with left_coloumn:
        st.subheader('Данные собраны:')
        st.subheader(end_date)

    with middle_column:
        st.subheader('Всего вакансий:')
        st.subheader(num_vacs)

    with right_column:
        st.subheader('Средняя зарплата:')
        st.subheader(f"{avg_salary} Р")

    st.markdown('---')

    # Названия графиков
    left_coloumn, right_column = st.columns(2)

    with left_coloumn:
        st.subheader('Самые популярные навыки')
    with right_column:
        st.subheader('Самые большие зарплаты')

    # Строим фильтры для графиков
    skill_type = vacs.loc[vacs['skill_type'] != '0']['skill_type'].unique().tolist()
    skill_type.sort()
    skill_type.insert(0, 'Все')

    employment_dict = {'full': 'Полная', 'part': 'Частичная', 'project': ' Проект', 'probation': 'Стажировка'}
    vacs['employment_rus'] = vacs['employment'].apply(lambda x: employment_dict[x])

    employment_types = vacs['employment_rus'].unique().tolist()
    employment_types.sort()
    employment_types.insert(0, 'Любая')

    left_coloumn, right_column = st.columns(2)
    with left_coloumn:
        skill_selection = st.multiselect('Тип навыка:',
                                    skill_type,
                                    default='Все')

        if "Все" in skill_selection:
            skill_selection = skill_type

    with right_column:
        employment_selection = st.multiselect('Тип занятости:',
                                    employment_types,
                                    default='Любая')

        if "Любая" in employment_selection:
            employment_selection = employment_types

    # Строим график для навыков
    if num_results == 'Топ 10':
        colormap = n_colors('rgb(250, 235, 215)', 'rgb(189, 147, 255)', 10, colortype = 'rgb')
        mask = selection['skill_type'].isin(skill_selection)
        gr = selection[mask].groupby(by=['skill_right_name'], as_index=False)['id_vac'].count().sort_values(by='id_vac', ascending=False)[:10]
        bar_chart_skills = px.bar(gr, 
                        x='id_vac',
                        y='skill_right_name',
                        color='skill_right_name',
                        orientation='h',
                        color_discrete_sequence=colormap,
                        text_auto=True,
                        template='plotly_white')
        bar_chart_skills.update_layout(showlegend=False)
        bar_chart_skills.update_layout(yaxis_title=None)
        bar_chart_skills.update_layout(xaxis_title=None)
        bar_chart_skills.update_layout(yaxis = dict(tickfont = dict(size=15)), xaxis = dict(tickfont = dict(size=14)))
    else:
        colormap = n_colors('rgb(250, 235, 215)', 'rgb(189, 147, 255)', 20, colortype = 'rgb')
        mask = selection['skill_type'].isin(skill_selection)
        gr = selection[mask].groupby(by=['skill_right_name'], as_index=False)['id_vac'].count().sort_values(by='id_vac', ascending=False)[:20]
        bar_chart_skills = px.bar(gr, 
                        x='id_vac',
                        y='skill_right_name',
                        color='skill_right_name',
                        orientation='h',
                        color_discrete_sequence=colormap,
                        text_auto=True,
                        template='plotly_white')
        bar_chart_skills.update_layout(showlegend=False)
        bar_chart_skills.update_layout(yaxis_title=None)
        bar_chart_skills.update_layout(xaxis_title=None)

    selection_salary = vacs.query(
        "city == @city_selection & employment_rus == @employment_selection"
    )

    y = selection_salary['grade'].unique()
    y_labels=[]
    i = 0
    for label in y:
        if 'Финансовый аналитик, инвестиционный аналитик' in label:
            y_labels.append(label.replace(', ',',<br>'))
        else:
            y_labels.append(label)

    if num_results == 'Топ 10':
        group_grade = selection_salary[selection_salary['salary'] != 0].groupby(by=['grade'], as_index=False)['salary'].mean().sort_values(by='salary', ascending=False)[:10]
    else:
        group_grade = selection_salary[selection_salary['salary'] != 0].groupby(by=['grade'], as_index=False)['salary'].mean().sort_values(by='salary', ascending=False)[:20]

    #  График для зарплат
    bar_chart = px.bar(group_grade, 
                    x='salary',
                    y='grade',
                    color='grade',
                    orientation='h',
                    color_discrete_sequence=colormap,
                    text_auto=True,
                    template='plotly_white')
    bar_chart.update_layout(showlegend=False)
    bar_chart.update_layout(yaxis_title=None)
    bar_chart.update_layout(xaxis_title=None)
    bar_chart.update_layout(yaxis = dict(tickfont = dict(size=14)), xaxis = dict(tickfont = dict(size=14)))
    bar_chart.update_yaxes(tickvals=y, ticktext=y_labels)
    bar_chart.update_xaxes(tickformat='.3s')

    left_bar, right_bar = st.columns(2)

    left_bar.plotly_chart(bar_chart_skills, use_container_width=True)
    right_bar.plotly_chart(bar_chart, use_container_width=True)

except:
    st.subheader("Ничего не нашлось(")

hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
"""
st.markdown(hide_st_style, unsafe_allow_html=True)
