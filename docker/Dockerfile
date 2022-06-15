FROM golang:latest

MAINTAINER snower sujian199@gmail.com

VOLUME /slock

EXPOSE 5658

WORKDIR /slock

COPY ./startup.sh /opt

RUN mkdir /slock/data \
    && chmod +x /opt/startup.sh \
    && cd /opt \
    && git clone https://github.com/snower/slock.git \
    && cd ./slock \
    && go build \
    && cp ./slock /go/bin/

CMD /opt/startup.sh