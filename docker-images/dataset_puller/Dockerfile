FROM python:3.8
WORKDIR /app

RUN mkdir /tmp/data
RUN mkdir /root/.kaggle 

ADD ./docker-images/dataset_puller/source .
COPY ./kaggle.json /root/.kaggle/kaggle.json

RUN pip install -r requirements.txt
RUN chmod 600 /root/.kaggle/kaggle.json

CMD ["python3", "data_pulling.py"]