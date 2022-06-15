FROM golang:latest

MAINTAINER snower sujian199@gmail.com

VOLUME /slock

EXPOSE 5658

WORKDIR /slock

COPY ./startup.sh /opt

RUN mkdir /slock/data \
    && chmod +x /opt/startup.sh \
    && go install -v github.com/snower/slock@latest

CMD /opt/startup.sh