FROM python:3.8
WORKDIR /app

ADD ./docker-images/database_creator/source .
COPY ./.env ./.env 

RUN pip install -r requirements.txt

CMD ["python3", "db_create.py"]