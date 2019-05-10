FROM python:3.7.3-alpine
LABEL maintainer="d.pronkin@anchorfree.com"
WORKDIR /usr/src/app
COPY requirements.txt ./
COPY *.py ./
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "./server.py" ]
