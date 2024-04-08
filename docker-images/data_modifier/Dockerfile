FROM python:3.8
WORKDIR /app

ADD ./docker-images/data_modifier/source .

RUN pip install -r requirements.txt

CMD ["python3", "data_modifier.py"]