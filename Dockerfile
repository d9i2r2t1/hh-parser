FROM python:3.8-slim-buster

COPY . ./hh_parser_v2
WORKDIR /hh_parser_v2

RUN apt-get update && apt-get install openssl && apt-get install ca-certificates
RUN pip install -e .