FROM python:3.8-slim-buster

ARG WORKDIR="/ubud"

RUN apt-get update && apt-get install -y git
RUN python -m pip install --upgrade pip

WORKDIR ${WORKDIR}
COPY . ${WORKDIR}
RUN rm .env

RUN python -m pip install . 

CMD ["ubud"]