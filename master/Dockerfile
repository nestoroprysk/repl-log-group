FROM python:3.7-alpine

WORKDIR /master

ENV FLASK_APP server.py

COPY . .

RUN pip3 install -r requirements.txt

CMD ["sh", "-c", "python3 -m flask run --host=0.0.0.0 --port=${PORT}"]
