package org.springframework.cloud.gateway.sample.config;


import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

import java.util.Objects;

@Configuration
public class GatewayConfig {

	@Bean
	public KeyResolver ipKeyResolver() {
		return exchange -> {
			// 获取请求的 IP
			return Mono.just(Objects.requireNonNull(exchange.getRequest().getRemoteAddress()).getAddress().getHostAddress());
		};
	}


}
