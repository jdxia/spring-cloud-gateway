package org.springframework.cloud.gateway.sample.predicates;

import org.springframework.cloud.gateway.handler.predicate.RoutePredicateFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

import java.util.List;
import java.util.function.Predicate;

@Component
public class Test1RoutePredicateFactory  implements RoutePredicateFactory<Test1RoutePredicateFactory.Config> {

	@Override
	public String name() {
		return "Test1Predicate";
	}

	@Override
	public List<String> shortcutFieldOrder() {
		return List.of("methodName");
	}

	@Override
	public Predicate<ServerWebExchange> apply(Config config) {
		return new Predicate<ServerWebExchange>() {
			@Override
			public boolean test(ServerWebExchange serverWebExchange) {
				return serverWebExchange.getRequest().getMethod().name().equalsIgnoreCase(config.getMethodName());
			}
		};
	}

	@Override
	public Class<Config> getConfigClass() {
		return Config.class;
	}

	public static class Config {
		private String methodName;

		public String getMethodName() {
			return methodName;
		}

		public void setMethodName(String methodName) {
			this.methodName = methodName;
		}
	}
}
