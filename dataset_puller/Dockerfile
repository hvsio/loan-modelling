FROM python:3.8
WORKDIR /scripts
COPY . /scripts
ADD requirements.txt .
RUN pip install -r requirements.txt
RUN mkdir /tmp/data
CMD ["python", "./data_pulling.py"]