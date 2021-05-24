FROM python:3.8.3-slim

COPY ./requirements.txt /usr/requirements.txt

WORKDIR /usr

RUN pip3 install -r requirements.txt

COPY ./src /usr/src
COPY  ./data /usr/data

ENTRYPOINT [ "python3" ]

CMD [ "src/app/main_recomendacao_v2.py" ]


