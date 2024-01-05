FROM python:3.10.13

WORKDIR /app

COPY . /app/dist

RUN pip install /app/dist

RUN rm -R /app/dist
