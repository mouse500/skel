package com.sec.vd.skel;

import io.lettuce.core.api.reactive.RedisReactiveCommands;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;

@SpringBootApplication
public class SkelApplication {

	public static void main(String[] args) {
		SpringApplication.run(SkelApplication.class, args);
	}

	@Bean
	public RouterFunction<ServerResponse> routes(SkelHandler skelHandler) {
		return RouterFunctions.route()
				.GET("/ping", skelHandler::ping)
				.GET("/ping_redis", skelHandler::ping_redis)
				.build();
	}




}
