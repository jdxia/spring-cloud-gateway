package org.springframework.cloud.gateway.sample.filter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 名字不限, 继承GlobalFilter, 全局生效
 */
@Component
@Slf4j
public class MyGlobalFilter implements GlobalFilter {


	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		String path = exchange.getRequest().getPath().value();
		String token = exchange.getRequest().getHeaders().getFirst("token");


		log.info("全局过滤器 ===> MyGlobalFilter, path: {}, token: {}", path, token);


		return chain.filter(exchange);
	}
}
