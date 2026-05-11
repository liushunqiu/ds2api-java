package com.ds2api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class Ds2ApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(Ds2ApiApplication.class, args);
    }
}
