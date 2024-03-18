FROM python:3.8
WORKDIR /app

ADD ./data_modifier/source .
COPY ./.env ./.env 

RUN pip install -r requirements.txt

CMD ["python3", "data_modifier.py"]