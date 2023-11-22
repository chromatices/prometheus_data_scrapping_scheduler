FROM python:3.9-slim
LABEL email='bomebug15@keti.re.kr'

RUN apt-get update
RUN apt install git -y

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Seoul
RUN apt-get install -y tzdata
RUN ["apt-get" ,"install" , "-y" ,"vim"]

ENV HOME=/home/
ENV PYTHONUNBUFFERED=1

RUN mkdir -p ${HOME}/scrapper
WORKDIR ${HOME}/scrapper

COPY ./src .

RUN pip3 install -r requirements.txt
RUN pip install git+https://github.com/casics/nostril.git

USER root

CMD ["python", "scheduler.py"]
