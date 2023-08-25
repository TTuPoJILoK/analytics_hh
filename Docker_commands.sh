# Создаем контейнер
docker build -t analytics_app:latest .

# Запускаем контейнер
docker run --name my_app -p 8501:8501 analytics_app:latest 

docker run  --name analytics_app -d -p 8501:8501 analytics_app  /bin/bash -c "python -m alembic upgrade head && python -m streamlit run app.py"
