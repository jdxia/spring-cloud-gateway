package org.springframework.cloud.gateway.sample.filter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.factory.GatewayFilterFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;

@Component
@Slf4j
public class Test1GatewayFilterFactory implements GatewayFilterFactory<Test1GatewayFilterFactory.Config> {

	@Override
	public String name() {
		// 过滤器的名字
		return "Test1";
	}

	@Override
	public Class<Config> getConfigClass() {
		return Test1GatewayFilterFactory.Config.class;
	}

	@Override
	public List<String> shortcutFieldOrder() {
		// 外面可以写成 - Test1=abc  这个abc就是赋值给prefix
		return List.of("prefix");
	}

	@Override
	public GatewayFilter apply(Config config) {
		return new GatewayFilter() {
			@Override
			public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
				System.out.println("前缀是: " + config.getPrefix());
				System.out.println("接受到一个请求: " + exchange);
				return chain.filter(exchange);
			}
		};
	}

	public static class Config {
		private String prefix;

		public String getPrefix() {
			return prefix;
		}

		public void setPrefix(String prefix) {
			this.prefix = prefix;
		}
	}
}
