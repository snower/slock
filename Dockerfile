FROM golang:latest as build

RUN cd /opt \
    && git clone https://github.com/snower/slock.git \
    && cd ./slock \
    && go build

FROM alpine:latest

MAINTAINER snower sujian199@gmail.com

VOLUME /slock

EXPOSE 5658

WORKDIR /slock

COPY --from=build /opt/slock/slock /usr/local/bin/
COPY docker/startup.sh /opt

RUN mkdir /slock/data && chmod +x /opt/startup.sh

CMD /opt/startup.sh