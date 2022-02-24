FROM python:3.8
COPY ml_pipelines/requirements.txt .
RUN pip install -r requirements.txt
RUN python3 setup.py install
COPY ml_pipelines ./ml_pipelines