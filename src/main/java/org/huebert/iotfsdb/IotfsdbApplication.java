package org.huebert.iotfsdb;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@OpenAPIDefinition(
    info = @Info(
        title = "iotfsdb",
        summary = "Time series database that leverages the unique properties of IOT data to improve the efficiency of storage size and retrieval time.",
        license = @License(
            name = "MIT",
            url = "https://github.com/jhuebert/iotfsdb/blob/main/LICENSE"
        ),
        contact = @Contact(
            name = "iotfsdb",
            url = "https://github.com/jhuebert/iotfsdb"
        )
    )
)
@EnableScheduling
@SpringBootApplication
public class IotfsdbApplication {

    public static void main(String[] args) {
        SpringApplication.run(IotfsdbApplication.class, args);
    }

}
