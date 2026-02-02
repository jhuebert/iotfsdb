FROM amazoncorretto:25.0.2-alpine
EXPOSE 8080
VOLUME /data
ENV IOTFSDB_PERSISTENCE_ROOT=/data
COPY iotfsdb.jar /iotfsdb.jar
CMD ["java", "-jar", "/iotfsdb.jar"]
