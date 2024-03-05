FROM python:3.8
WORKDIR /scripts
COPY . /scripts
RUN pip install kaggle && mkdir data
CMD ["python", "./data_pulling.py"]