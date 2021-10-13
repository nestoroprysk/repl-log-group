FROM python:3.7-alpine

ENV FLASK_APP server.py

EXPOSE 8080

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0", "--port=8008" ]