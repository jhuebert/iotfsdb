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
        summary = "",
        description = "",
        license = @License(
            name = "MIT",
            url = "https://www.github.com"
        ),
        contact = @Contact(
            name = "Jason Huebert",
            url = "https://www.github.com/jhuebert",
            email = "n/a"
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
