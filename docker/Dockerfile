FROM amazoncorretto:24-alpine
EXPOSE 8080
VOLUME /data
ENV IOTFSDB_ROOT=/data
COPY iotfsdb.jar /
ENTRYPOINT ["java", "-jar", "/iotfsdb.jar"]
