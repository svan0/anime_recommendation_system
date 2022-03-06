FROM python:3.8
RUN pip install tensorflow==2.7.0
RUN pip install tensorflow-recommenders==0.6.0
RUN pip install tensorflow-ranking==0.5.0
RUN pip install google-cloud-storage==1.43.0
RUN pip install google-cloud-bigquery==2.31.0
RUN pip install numpy==1.22.0
RUN pip install pandas==1.3.4
RUN pip install pyarrow==6.0.1
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY anime_rec_pkg/ ./anime_rec_pkg
WORKDIR "/anime_rec_pkg"