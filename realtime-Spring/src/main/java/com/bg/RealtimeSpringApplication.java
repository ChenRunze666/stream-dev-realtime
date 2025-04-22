package com.bg;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.bg.mapper")
public class RealtimeSpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(RealtimeSpringApplication.class, args);
    }

}
