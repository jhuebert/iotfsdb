package org.huebert.iotfsdb;

import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.series.PartitionPeriod;
import org.huebert.iotfsdb.series.SeriesDefinition;
import org.huebert.iotfsdb.series.SeriesType;
import org.huebert.iotfsdb.service.SeriesService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.UUID;

@SpringBootApplication
public class IotfsdbApplication {

    public static void main(String[] args) {
        SpringApplication.run(IotfsdbApplication.class, args);
    }

    @Slf4j
    @Component
    public static class Clr implements CommandLineRunner {

        private final SeriesService seriesService;

        public Clr(SeriesService seriesService) {
            this.seriesService = seriesService;
        }

        @Override
        public void run(String... args) throws Exception {
//            for (int i = 0; i < 100; i++) {
//                createSeries();
//            }
//            log.info("done");
        }

        private void createSeries() throws IOException {
            int valueInterval = 60;
            SeriesDefinition seriesDefinition = new SeriesDefinition("float-month-60-" + UUID.randomUUID(), SeriesType.FLOAT, valueInterval, PartitionPeriod.MONTH);
            log.info("series: {}", seriesDefinition.id());
            seriesService.createSeries(seriesDefinition);
            ZonedDateTime end = ZonedDateTime.now();
            log.info("end: {}", end);
            ZonedDateTime dateTime = end.minusMonths(12);
            log.info("start: {}", dateTime);
            while (dateTime.isBefore(end)) {
                seriesService.set(seriesDefinition.id(), dateTime, String.valueOf(Math.random() * 10.0 + 60.0));
                dateTime = dateTime.plusSeconds(valueInterval);
            }
        }
    }

}
