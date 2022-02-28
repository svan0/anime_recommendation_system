FROM python:3.8
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY anime_rec_pkg/ ./anime_rec_pkg
WORKDIR "/anime_rec_pkg"