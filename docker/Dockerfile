FROM amazoncorretto:21-alpine-jdk
EXPOSE 8080
VOLUME /data
ENV IOTFSDB_ROOT=/data
ENV IOTFSDB_READ_ONLY=false
COPY iotfsdb.jar /
ENTRYPOINT ["java", "-jar", "/iotfsdb.jar"]
