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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.event.RefreshRoutesEvent;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.filter.OrderedGatewayFilter;
import org.springframework.cloud.gateway.filter.factory.GatewayFilterFactory;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.context.ApplicationListener;
import org.springframework.core.DecoratingProxy;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.Order;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebHandler;

import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR;

/**
 * WebHandler that delegates to a chain of {@link GlobalFilter} instances and
 * {@link GatewayFilterFactory} instances then to the target {@link WebHandler}.
 *
 * @author Rossen Stoyanchev
 * @author Spencer Gibb
 * @author Yuxin Wang
 * @since 0.1
 */
public class FilteringWebHandler implements WebHandler, ApplicationListener<RefreshRoutesEvent> {

	protected static final Log logger = LogFactory.getLog(FilteringWebHandler.class);

	/**
	 * 全局过滤器
	 */
	private final List<GatewayFilter> globalFilters;

	private final ConcurrentHashMap<Route, List<GatewayFilter>> routeFilterMap = new ConcurrentHashMap();

	private final boolean routeFilterCacheEnabled;

	@Deprecated
	public FilteringWebHandler(List<GlobalFilter> globalFilters) {
		this(globalFilters, false);
	}

	public FilteringWebHandler(List<GlobalFilter> globalFilters, boolean routeFilterCacheEnabled) {
		// 获取所有全局过滤器
		this.globalFilters = loadFilters(globalFilters);
		this.routeFilterCacheEnabled = routeFilterCacheEnabled;
	}

	/* for testing */ ConcurrentHashMap<Route, List<GatewayFilter>> getRouteFilterMap() {
		return routeFilterMap;
	}

	/**
	 * 此方法主要是将GlobalFilter适配为GatewayFilter
	 * @param filters
	 * @return
	 */
	private static List<GatewayFilter> loadFilters(List<GlobalFilter> filters) {
		return filters.stream().map(filter -> {

			// 通过GatewayFilterAdapter将GlobalFilter适配为GatewayFilter
			GatewayFilterAdapter gatewayFilter = new GatewayFilterAdapter(filter);

			// 判断GlobalFilter是否实现了Ordered接口
			if (filter instanceof Ordered ordered) {
				int order = ordered.getOrder();

				/**
				 * OrderedGatewayFilter是一个有序的网关过滤器实现类，在FilterChain，过滤器数组会首先按照order进行顺序排序，按顺序过滤请求
				 * 返回OrderedGatewayFilter
				 */
				return new OrderedGatewayFilter(gatewayFilter, order);
			}
			else {
				Order order = AnnotationUtils.findAnnotation(filter.getClass(), Order.class);
				if (order != null) {
					return new OrderedGatewayFilter(gatewayFilter, order.value());
				}
			}
			return gatewayFilter;
		}).collect(Collectors.toList());
	}

	@Override
	public void onApplicationEvent(RefreshRoutesEvent event) {
		if (this.routeFilterCacheEnabled) {
			routeFilterMap.clear();
		}
	}

	@Override
	public Mono<Void> handle(ServerWebExchange exchange) {
		/**
		 * 获取匹配到的路由
		 */
		Route route = exchange.getRequiredAttribute(GATEWAY_ROUTE_ATTR);

		/**
		 * 获取所有filter, 并排序好
		 * 包括全局过滤器
		 */
		List<GatewayFilter> combined = getCombinedFilters(route);

		if (logger.isDebugEnabled()) {
			logger.debug("Sorted gatewayFilterFactories: " + combined);
		}

		/**
		 * 责任链, 从外到里 依次执行, combined 这是个list
		 *
		 * 如果下游响应的话, 从里到外触发 doOnSuccess / doOnError 等回调
		 */
		return new DefaultGatewayFilterChain(combined).filter(exchange);
	}

	protected List<GatewayFilter> getCombinedFilters(Route route) {
		// 如果开启缓存
		if (this.routeFilterCacheEnabled) {
			// 这个map的value是list, 是排序好的
			return routeFilterMap.computeIfAbsent(route, this::getAllFilters);
		}
		else {
			// 往下
			return getAllFilters(route);
		}
	}

	// 所有的过滤器
	protected List<GatewayFilter> getAllFilters(Route route) {
		// 路由的过滤器
		List<GatewayFilter> gatewayFilters = route.getFilters();
		// 全局的过滤器
		List<GatewayFilter> combined = new ArrayList<>(this.globalFilters);
		combined.addAll(gatewayFilters);

		// 排序
		AnnotationAwareOrderComparator.sort(combined);
		return combined;
	}

	private static class DefaultGatewayFilterChain implements GatewayFilterChain {

		private final int index;

		private final List<GatewayFilter> filters;

		DefaultGatewayFilterChain(List<GatewayFilter> filters) {
			this.filters = filters;
			// 在FilteringWebHandler的handle方法中初始化时设置当前应调用的过滤器下标为0，也就是第一个
			this.index = 0;
		}

		//在filter方法中调用，传入过滤器链和需要执行的过滤器index
		private DefaultGatewayFilterChain(DefaultGatewayFilterChain parent, int index) {
			this.filters = parent.getFilters();
			this.index = index;
		}

		public List<GatewayFilter> getFilters() {
			return filters;
		}

		@Override
		public Mono<Void> filter(ServerWebExchange exchange) {
			return Mono.defer(() -> {
				//判断是否已经执行过所有的过滤器
				if (this.index < filters.size()) {
					//取出当前需执行的过滤器
					GatewayFilter filter = filters.get(this.index);
					//每个Filter都创建一个DefaultGatewayFilterChain去执行
					DefaultGatewayFilterChain chain = new DefaultGatewayFilterChain(this, this.index + 1);
					return filter.filter(exchange, chain);
				}
				else {
					return Mono.empty(); // complete
				}
			});
		}

	}

	// 网关过滤器链默认实现类
	private static class GatewayFilterAdapter implements GatewayFilter, DecoratingProxy {

		// 委托的 GlobalFilter
		private final GlobalFilter delegate;

		// 使用 delegate 过滤请求
		GatewayFilterAdapter(GlobalFilter delegate) {
			this.delegate = delegate;
		}

		@Override
		public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
			return this.delegate.filter(exchange, chain);
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder("GatewayFilterAdapter{");
			sb.append("delegate=").append(delegate);
			sb.append('}');
			return sb.toString();
		}

		@Override
		public Class<?> getDecoratedClass() {
			return delegate.getClass();
		}

	}

}
