FROM python:3.8-slim-buster

ARG WORKDIR="/ubud"

RUN apt-get update && apt-get install -y git
RUN pip install --upgrade pip

WORKDIR ${WORKDIR}
COPY . ${WORKDIR}
RUN rm -f .env

RUN pip install . 

CMD ["ubud"]