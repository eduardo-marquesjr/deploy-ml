FROM python:3.8.3-slim

ENV GOOGLE_APPLICATION_CREDENTIALS="src/pristine-bonito-301012-7eb9e317fe07.json"

COPY ./requirements.txt /usr/requirements.txt

WORKDIR /usr

RUN pip3 install -r requirements.txt

COPY ./src /usr/src
COPY  ./data /usr/data

ENTRYPOINT [ "python3" ]

CMD [ "src/app/main_recomendacao_v2.py" ]


