FROM amazoncorretto:25.0.1-alpine
EXPOSE 8080
VOLUME /data
ENV IOTFSDB_PERSISTENCE_ROOT=/data
COPY iotfsdb.jar /iotfsdb.jar
CMD ["java", "-jar", "/iotfsdb.jar"]
