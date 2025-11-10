FROM amazoncorretto:11-alpine-jdk
WORKDIR /home
COPY . .
ENTRYPOINT ["/home/service/bin/service"]