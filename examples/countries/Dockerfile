FROM python:alpine3.6

RUN apk update && apk add git

RUN mkdir /srv/example/
COPY src/. /srv/example/
WORKDIR /srv/example/

RUN pip install -r requirements.txt
CMD python $WORKLOAD_EXECUTABLE
