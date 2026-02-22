package com.study;

import com.study.nacos.NacosManualService;
import jakarta.annotation.Resource;
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

	@Resource
	private NacosManualService nacosManualService;

	private static final Logger log = LoggerFactory.getLogger(UserApplication.class);

	@Bean
	public RouterFunction<ServerResponse> test() {
		// http://127.0.0.1:8890/api/user-demo/test 可以再开一个 http://127.0.0.1:8890/api/user-demo/test
		return route()
				.GET("/test", request -> {
					log.info("===========> get user success! time: {}", LocalDateTime.now());
					return ServerResponse.status(HttpStatus.OK).body("get user success!");
				})
				.build();
	}

	@Bean
	public RouterFunction<ServerResponse> testOnline() {
		// http://127.0.0.1:8890/api/user-demo/online
		return route()
				.GET("/online", request -> {
					nacosManualService.online();
					log.info("===========> online");
					return ServerResponse.status(HttpStatus.OK).body("online!");
				})
				.build();
	}

	@Bean
	public RouterFunction<ServerResponse> testOffline() {
		// http://127.0.0.1:8890/api/user-demo/offline
		return route()
				.GET("/offline", request -> {
					nacosManualService.offline();
					log.info("===========> offline");
					return ServerResponse.status(HttpStatus.OK).body("offline!");
				})
				.build();
	}

	public static void main(String[] args) {
		System.setProperty("nacos.logging.default.config.enabled", "false");
		System.setProperty("rocketmq.client.logUseSlf4j", "true");

		SpringApplication.run(UserApplication.class, args);
	}


}
