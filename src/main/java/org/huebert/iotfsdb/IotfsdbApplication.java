package org.huebert.iotfsdb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class IotfsdbApplication {

    public static void main(String[] args) {
        SpringApplication.run(IotfsdbApplication.class, args);
    }

}
