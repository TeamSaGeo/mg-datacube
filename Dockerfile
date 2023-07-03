FROM python:3.10

RUN apt-get update
RUN apt-get install -y make automake gcc g++ subversion python3-dev libgdal-dev
RUN pip install --upgrade pip

RUN mkdir wd
WORKDIR wd
COPY ./requirements.txt .

RUN pip3 install -r requirements.txt

COPY ./ ./

CMD [ "gunicorn", "--workers=5", "--threads=5", "--preload", "-b 0.0.0.0:80", "setup:server", "--timeout=120" ]

# dev environment
# CMD ["python", "setup.py"]
# RUN apk add nano libpq-dev python3-dev binutils libgdal-dev python3-gdal build-essential gdal-bin
