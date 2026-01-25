package com.study;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerResponse;

import java.time.LocalDateTime;

import static org.springframework.web.servlet.function.RouterFunctions.route;

@SpringBootApplication
public class UserApplication {

	private static final Logger log = LoggerFactory.getLogger(UserApplication.class);

	@Bean
	public RouterFunction<ServerResponse> user() {
		// http://127.0.0.1:8890/test 可以再开一个 http://127.0.0.1:8890/test
		return route()
				.GET("/test", request -> {
					log.info("===========> get user success! time: {}", LocalDateTime.now());
					return ServerResponse.status(HttpStatus.OK).body("get user success!");
				})
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(UserApplication.class, args);
	}
}
