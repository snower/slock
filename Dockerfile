FROM golang:latest as build

RUN cd /opt && git clone https://github.com/snower/slock.git && cd ./slock && go build

FROM debian:latest

MAINTAINER snower sujian199@gmail.com

VOLUME /slock/data

EXPOSE 5658

WORKDIR /slock

COPY --from=build /opt/slock/slock /usr/local/bin/
COPY --from=redis:latest /usr/local/bin/redis-cli /usr/local/bin/
COPY docker/startup.sh /opt

RUN apt update && apt install -y openssl && chmod +x /opt/startup.sh

CMD /opt/startup.sh