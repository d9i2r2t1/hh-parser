FROM python:3.8-slim-buster

COPY . ./hh_parser_v2
WORKDIR /hh_parser_v2

RUN apt-get update && apt-get install openssl && apt-get install ca-certificates
RUN apt-get -yqq install libpq-dev python-dev
RUN apt -yqq install build-essential
RUN apt-get -yqq install manpages-dev

RUN pip install -e .