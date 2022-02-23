FROM python:3.8
COPY ml_pipelines/requirements.txt .
RUN pip install -r requirements.txt
RUN python3 -m pip install ml_package/
COPY ml_pipelines ./ml_pipelines