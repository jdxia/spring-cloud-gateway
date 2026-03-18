/*
 * Copyright 2013-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.gateway.handler;

import java.util.function.Function;

import org.springframework.cloud.gateway.route.CachingRouteLocator;
import org.springframework.web.reactive.DispatcherHandler;
import org.springframework.web.reactive.result.SimpleHandlerAdapter;
import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.config.GatewayProperties;
import org.springframework.cloud.gateway.config.GlobalCorsProperties;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.env.Environment;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.reactive.handler.AbstractHandlerMapping;
import org.springframework.web.server.ServerWebExchange;

import static org.springframework.cloud.gateway.handler.RoutePredicateHandlerMapping.ManagementPortType.DIFFERENT;
import static org.springframework.cloud.gateway.handler.RoutePredicateHandlerMapping.ManagementPortType.DISABLED;
import static org.springframework.cloud.gateway.handler.RoutePredicateHandlerMapping.ManagementPortType.SAME;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_HANDLER_MAPPER_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_PREDICATE_ROUTE_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_REACTOR_CONTEXT_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR;

/**
 * @author Spencer Gibb
 */
public class RoutePredicateHandlerMapping extends AbstractHandlerMapping {

	private final FilteringWebHandler webHandler;

	/**
	 * {@link CachingRouteLocator}
	 */
	private final RouteLocator routeLocator;

	private final Integer managementPort;

	private final ManagementPortType managementPortType;

	public RoutePredicateHandlerMapping(FilteringWebHandler webHandler, RouteLocator routeLocator,
			GlobalCorsProperties globalCorsProperties, Environment environment) {
		this.webHandler = webHandler;
		this.routeLocator = routeLocator;

		this.managementPort = getPortProperty(environment, "management.server.");
		this.managementPortType = getManagementPortType(environment);
		// RequestMappingHandlerMapping 之后
		setOrder(environment.getProperty(GatewayProperties.PREFIX + ".handler-mapping.order", Integer.class, 1));
		setCorsConfigurations(globalCorsProperties.getCorsConfigurations());
	}

	private ManagementPortType getManagementPortType(Environment environment) {
		Integer serverPort = getPortProperty(environment, "server.");
		if (this.managementPort != null && this.managementPort < 0) {
			return DISABLED;
		}
		return ((this.managementPort == null || (serverPort == null && this.managementPort.equals(8080))
				|| (this.managementPort != 0 && this.managementPort.equals(serverPort))) ? SAME : DIFFERENT);
	}

	private static Integer getPortProperty(Environment environment, String prefix) {
		return environment.getProperty(prefix + "port", Integer.class);
	}

	/**
	 * 核心
	 *
	 * 调用链路
	 *   HTTP 请求进入
	 *       ↓
	 *  {@link DispatcherHandler#handle(ServerWebExchange)}  Spring WebFlux 核心调度器
	 *       ↓
	 *  {@link AbstractHandlerMapping#getHandler(ServerWebExchange)}    [父类模板方法] 遍历所有 HandlerMapping，调用 getHandler(), 会 在 getHandlerInternal 之后 处理 CORS
	 *       ↓
	 *  {@link AbstractHandlerMapping#getHandlerInternal(ServerWebExchange)}  [抽象方法]
	 *       ↓
	 *  {@link RoutePredicateHandlerMapping#getHandlerInternal(ServerWebExchange)}  [子类实现]
	 *        ↓
	 *  上面的会 返回 webHandler，它是 {@link FilteringWebHandler} 实例,
	 *  然后 {@link DispatcherHandler#handle(ServerWebExchange)} 这个里面的 handleRequestWith 会进行处理,
	 *  入参的handler就是 上面的 webHandler,
	 *  然后进入到 {@link DispatcherHandler#handleRequestWith(ServerWebExchange, Object)} , 里面的 adapter 是 {@link SimpleHandlerAdapter} 可以看这个 {@link SimpleHandlerAdapter#supports(Object)}
	 * 具体处理是 {@link SimpleHandlerAdapter#handle(ServerWebExchange, Object)}  也就是 调用这个handler的handle方法 {@link FilteringWebHandler#handle(ServerWebExchange)}
	 *    filter handle里面里面会有正向和逆向, 函数式编程实现的
	 *
	 */
	@Override
	protected Mono<?> getHandlerInternal(ServerWebExchange exchange) {
		/**
		 * 管理端口处理
		 * 如果配置了独立的管理端口(如actuator)，且当前请求来自管理端口
		 * 则不处理该请求，返回empty让其他HandlerMapping处理
		 */
		// don't handle requests on management port if set and different than server port
		if (this.managementPortType == DIFFERENT && this.managementPort != null
				&& exchange.getRequest().getLocalAddress() != null
				&& exchange.getRequest().getLocalAddress().getPort() == this.managementPort) {
			// 返回empty = 当前Handler不处理
			return Mono.empty();
		}

		// GATEWAY_HANDLER_MAPPER_ATTR 记录是哪个HandlerMapping处理了这个请求，用于调试和追踪
		exchange.getAttributes().put(GATEWAY_HANDLER_MAPPER_ATTR, getSimpleName());

		return Mono.deferContextual(contextView -> {
			exchange.getAttributes().put(GATEWAY_REACTOR_CONTEXT_ATTR, contextView);

			/**
			 * 根据当前的请求匹配路由
			 * 从缓存里面获取所有路由, 调用每个路由器的匹配规则进行判断
			 */
			return lookupRoute(exchange)
				// .log("route-predicate-handler-mapping", Level.FINER) //name this

					// r 就是匹配到的路由, 匹配成功的处理
				.map((Function<Route, ?>) r -> {
					/**
					 * 清除临时的predicate路由ID属性
					 * 因为已经匹配成功，不需要再记录"正在测试哪个路由"
					 */
					exchange.getAttributes().remove(GATEWAY_PREDICATE_ROUTE_ATTR);
					if (logger.isDebugEnabled()) {
						logger.debug("Mapping [" + getExchangeDesc(exchange) + "] to " + r);
					}

					/**
					 * 把匹配到的路由缓存起来
					 * r 是 匹配到的 router
					 *
					 * GATEWAY_ROUTE_ATTR 放的就是 匹配成功后存储最终选中的路由对象, 路由信息
					 */
					exchange.getAttributes().put(GATEWAY_ROUTE_ATTR, r);

					/**
					 * 返回 webHandler，它是 {@link FilteringWebHandler} 实例
					 * Spring WebFlux会调用这个handler的handle方法 {@link FilteringWebHandler#handle(ServerWebExchange)}
					 */
					return webHandler;
				})
					// 如果没有匹配到路由, 返回为空的话就是 404
				.switchIfEmpty(Mono.empty().then(Mono.fromRunnable(() -> {
					exchange.getAttributes().remove(GATEWAY_PREDICATE_ROUTE_ATTR);

					/**
					 * 清除ServerWebExchange属性中的请求正文。该属性为CACHE_REQUEST_BODY_ATTR
					 */
					ServerWebExchangeUtils.clearCachedRequestBody(exchange);
					if (logger.isTraceEnabled()) {
						logger.trace("No RouteDefinition found for [" + getExchangeDesc(exchange) + "]");
					}
				})));
		});
	}

	@Override
	protected CorsConfiguration getCorsConfiguration(Object handler, ServerWebExchange exchange) {
		// TODO: support cors configuration via properties on a route see gh-229
		// see RequestMappingHandlerMapping.initCorsConfiguration()
		// also see
		// https://github.com/spring-projects/spring-framework/blob/master/spring-web/src/test/java/org/springframework/web/cors/reactive/CorsWebFilterTests.java
		return super.getCorsConfiguration(handler, exchange);
	}

	// TODO: get desc from factory?
	private String getExchangeDesc(ServerWebExchange exchange) {
		StringBuilder out = new StringBuilder();
		out.append("Exchange: ");
		out.append(exchange.getRequest().getMethod());
		out.append(" ");
		out.append(exchange.getRequest().getURI());
		return out.toString();
	}

	protected Mono<Route> lookupRoute(ServerWebExchange exchange) {
		/**
		 * 从RouteLocator获取所有路由定义
		 * routeLocator通常是CachingRouteLocator(带缓存)
		 */
		return this.routeLocator.getRoutes().filterWhen(route -> {

			/**
			 * 在predicate执行前，先设置当前正在测试的路由ID
			 * 这样predicate内部可以知道 当前是哪个路由在匹配
			 *
			 * GATEWAY_PREDICATE_ROUTE_ATTR: 路由匹配过程中临时存储正在测试的路由ID, 值是 String (routeId)
			 * 和 GATEWAY_ROUTE_ATTR 不一样, 这是匹配成功后存储最终选中的路由对象 值是  Route 对象
			 */
			// add the current route we are testing
			exchange.getAttributes().put(GATEWAY_PREDICATE_ROUTE_ATTR, route.getId());
			try {

				/**
				 * 调用每个路由器的匹配规则进行判断
				 * predicate.apply(exchange) 返回 Mono<Boolean>
				 */
				return route.getPredicate().apply(exchange);
			}
			catch (Exception e) {
				logger.error("Error applying predicate for route: " + route.getId(), e);
			}
			return Mono.just(false);
		})
				// .next() 只取第一个匹配的路由
			.next()
			// TODO: error handling
			.map(route -> {
				if (logger.isDebugEnabled()) {
					logger.debug("Route matched: " + route.getId());
				}

				// 验证路由(默认空实现，是扩展点)
				validateRoute(route, exchange);
				return route;
			});

		/*
		 * TODO: trace logging if (logger.isTraceEnabled()) {
		 * logger.trace("RouteDefinition did not match: " + routeDefinition.getId()); }
		 */
	}

	/**
	 * Validate the given handler against the current request.
	 * <p>
	 * The default implementation is empty. Can be overridden in subclasses, for example
	 * to enforce specific preconditions expressed in URL mappings.
	 * @param route the Route object to validate
	 * @param exchange current exchange
	 * @throws Exception if validation failed
	 */
	@SuppressWarnings("UnusedParameters")
	protected void validateRoute(Route route, ServerWebExchange exchange) {
	}

	protected String getSimpleName() {
		return "RoutePredicateHandlerMapping";
	}

	public enum ManagementPortType {

		/**
		 * The management port has been disabled.
		 */
		DISABLED,

		/**
		 * The management port is the same as the server port.
		 */
		SAME,

		/**
		 * The management port and server port are different.
		 */
		DIFFERENT;

	}

}
