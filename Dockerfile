FROM python:3.7.2-alpine
WORKDIR /usr/src/app
COPY requirements.txt ./
COPY *.py ./
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "./server.py" ]
