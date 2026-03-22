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

package org.springframework.cloud.gateway.route;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.gateway.filter.WeightCalculatorWebFilter;
import org.springframework.context.ApplicationEvent;
import reactor.core.publisher.Flux;

import org.springframework.cloud.gateway.config.GatewayProperties;
import org.springframework.cloud.gateway.event.FilterArgsEvent;
import org.springframework.cloud.gateway.event.PredicateArgsEvent;
import org.springframework.cloud.gateway.filter.FilterDefinition;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.OrderedGatewayFilter;
import org.springframework.cloud.gateway.filter.factory.GatewayFilterFactory;
import org.springframework.cloud.gateway.handler.AsyncPredicate;
import org.springframework.cloud.gateway.handler.predicate.PredicateDefinition;
import org.springframework.cloud.gateway.handler.predicate.RoutePredicateFactory;
import org.springframework.cloud.gateway.support.ConfigurationService;
import org.springframework.cloud.gateway.support.HasRouteId;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.util.CollectionUtils;
import org.springframework.web.server.ServerWebExchange;

/**
 * {@link RouteLocator} that loads routes from a {@link RouteDefinitionLocator}.
 *
 * @author Spencer Gibb
 */
public class RouteDefinitionRouteLocator implements RouteLocator {

	/**
	 * Default filters name.
	 */
	public static final String DEFAULT_FILTERS = "defaultFilters";

	protected final Log logger = LogFactory.getLog(getClass());

	private final RouteDefinitionLocator routeDefinitionLocator;

	private final ConfigurationService configurationService;

	/**
	 * RoutePredicateFactory 映射
	 * key ：{@link RoutePredicateFactory#name()}
	 */
	private final Map<String, RoutePredicateFactory> predicates = new LinkedHashMap<>();

	/**
	 * GatewayFilterFactory 映射
	 * key ：{@link GatewayFilterFactory#name()}
	 */
	private final Map<String, GatewayFilterFactory> gatewayFilterFactories = new HashMap<>();

	private final GatewayProperties gatewayProperties;

	public RouteDefinitionRouteLocator(RouteDefinitionLocator routeDefinitionLocator, // 一个 RouteDefinitionLocator 对象
			List<RoutePredicateFactory> predicates, // Predicate 工厂列表，会被映射成 key 为 name, value 为 factory 的 Map。可以猜想出 gateway 是如何根据 PredicateDefinition 中定义的 name 来匹配到相对应的 factory 了
			List<GatewayFilterFactory> gatewayFilterFactories,  // GatewayFilter 工厂列表，同样会被映射成 key 为 name, value 为 factory 的 Map
			GatewayProperties gatewayProperties, // 外部化配置类
									   ConfigurationService configurationService) {

		// 设置 RouteDefinitionLocator
		this.routeDefinitionLocator = routeDefinitionLocator;
		this.configurationService = configurationService;

		//初始化Predicate断言信息（所有的）
		initFactories(predicates);

		//初始化Filter信息（所有的），与初始化Predicate断言信息类似
		gatewayFilterFactories.forEach(factory -> this.gatewayFilterFactories.put(factory.name(), factory));

		// 设置 GatewayProperties
		this.gatewayProperties = gatewayProperties;
	}

	private void initFactories(List<RoutePredicateFactory> predicates) {
		predicates.forEach(factory -> {
			//key为RoutePredicateFactory实现类的名称前缀如AfterRoutePredicateFactory则key为After
			String key = factory.name();
			if (this.predicates.containsKey(key)) {
				this.logger.warn("A RoutePredicateFactory named " + key + " already exists, class: "
						+ this.predicates.get(key) + ". It will be overwritten.");
			}

			//如果已经存在该断言Factory，则覆盖，也就是说以SCG内置的为主
			this.predicates.put(key, factory);
			if (logger.isInfoEnabled()) {
				logger.info("Loaded RoutePredicateFactory [" + key + "]");
			}
		});
	}

	/**
	 * Filtering is done via {@link RouteDefinition} instead of {@link Route} to prevent
	 * creating Route instances that will be discarded.
	 */
	@Override
	public Flux<Route> getRoutesByMetadata(Map<String, Object> metadata) {
		return getRoutes(this.routeDefinitionLocator.getRouteDefinitions()
			.filter(routeDef -> RouteLocator.matchMetadata(routeDef.getMetadata(), metadata)));
	}

	@Override
	public Flux<Route> getRoutes() {
		// 往下
		return getRoutes(this.routeDefinitionLocator.getRouteDefinitions());
	}

	private Flux<Route> getRoutes(Flux<RouteDefinition> routeDefinitions) {
		// 每个路由定义挨个转换处理, 得到 route 对象
		Flux<Route> routes = routeDefinitions.map(this::convertToRoute);

		if (!gatewayProperties.isFailOnRouteDefinitionError()) {
			// instead of letting error bubble up, continue
			routes = routes.onErrorContinue((error, obj) -> {
				if (logger.isWarnEnabled()) {
					logger.warn("RouteDefinition id " + ((RouteDefinition) obj).getId()
							+ " will be ignored. Definition has invalid configs, " + error.getMessage());
				}
			});
		}

		return routes.map(route -> {
			if (logger.isDebugEnabled()) {
				logger.debug("RouteDefinition matched: " + route.getId());
			}
			return route;
		});
	}

	private Route convertToRoute(RouteDefinition routeDefinition) {
		/**
		 * 重点
		 * 获取RouteDefinition对应的断言
		 * 根据路由定义, 生成匹配器, 将 PredicateDefinition 转换成 AsyncPredicate
		 */
		AsyncPredicate<ServerWebExchange> predicate = combinePredicates(routeDefinition);
		// 根据路由定义, 生成过滤器
		List<GatewayFilter> gatewayFilters = getFilters(routeDefinition);

		// 构建路由对象
		return Route.async(routeDefinition).asyncPredicate(predicate).replaceFilters(gatewayFilters).build();
	}

	@SuppressWarnings("unchecked")
	List<GatewayFilter> loadGatewayFilters(String id, List<FilterDefinition> filterDefinitions) {
		ArrayList<GatewayFilter> ordered = new ArrayList<>(filterDefinitions.size());

		// 循环所有过滤器
		for (int i = 0; i < filterDefinitions.size(); i++) {
			FilterDefinition definition = filterDefinitions.get(i);

			// 根据定义的名字找出过滤器工厂
			GatewayFilterFactory factory = this.gatewayFilterFactories.get(definition.getName());
			if (factory == null) {
				throw new IllegalArgumentException(
						"Unable to find GatewayFilterFactory with name " + definition.getName());
			}
			if (logger.isDebugEnabled()) {
				logger.debug("RouteDefinition " + id + " applying filter " + definition.getArgs() + " to "
						+ definition.getName());
			}

			// 生成过滤器工厂里面的配置对象
			// @formatter:off
			Object configuration = this.configurationService.with(factory)
					.name(definition.getName())
					.properties(definition.getArgs())
					.eventFunction((bound, properties) -> new FilterArgsEvent(
							// TODO: why explicit cast needed or java compile fails
							RouteDefinitionRouteLocator.this, id, (Map<String, Object>) properties))
					.bind();
			// @formatter:on

			// some filters require routeId
			// TODO: is there a better place to apply this?
			if (configuration instanceof HasRouteId hasRouteId) {
				hasRouteId.setRouteId(id);
			}

			// 根据配置对象 生成过滤器, 生成GatewayFilter
			GatewayFilter gatewayFilter = factory.apply(configuration);
			if (gatewayFilter instanceof Ordered) {
				ordered.add(gatewayFilter);
			}
			else {
				//如果没有实现Ordered接口，则根据遍历的顺序排序
				ordered.add(new OrderedGatewayFilter(gatewayFilter, i + 1));
			}
		}

		// 返回 GatewayFilter 数组
		return ordered;
	}

	private List<GatewayFilter> getFilters(RouteDefinition routeDefinition) {
		List<GatewayFilter> filters = new ArrayList<>();

		// 获取默认过滤器的配置, 处理 GatewayProperties 中定义的默认的 FilterDefinition，转换成 GatewayFilter
		// TODO: support option to apply defaults after route specific filters?
		if (!this.gatewayProperties.getDefaultFilters().isEmpty()) {
			filters.addAll(loadGatewayFilters(routeDefinition.getId(),
					new ArrayList<>(this.gatewayProperties.getDefaultFilters())));
		}

		// 路由定义里面指定的过滤器对象
		final List<FilterDefinition> definitionFilters = routeDefinition.getFilters();

		// 将 RouteDefinition 中定义的 FilterDefinition 转换成 GatewayFilter
		if (!CollectionUtils.isEmpty(definitionFilters)) {
			filters.addAll(loadGatewayFilters(routeDefinition.getId(), definitionFilters));
		}

		// 排序, 对 GatewayFilter 进行排序
		AnnotationAwareOrderComparator.sort(filters);
		return filters;
	}

	private AsyncPredicate<ServerWebExchange> combinePredicates(RouteDefinition routeDefinition) {
		// 获取所有的匹配器
		List<PredicateDefinition> predicates = routeDefinition.getPredicates();
		if (predicates == null || predicates.isEmpty()) {
			// this is a very rare case, but possible, just match all
			return AsyncPredicate.from(exchange -> true);
		}

		return predicates.stream()
				// 调用 lookup 方法，将列表中第一个 PredicateDefinition 转换成 AsyncPredicate
			.map(nextPredicate -> lookup(routeDefinition, nextPredicate))
				// 迭代 将列表中每一个 PredicateDefinition 都转换成 AsyncPredicate, 每个都是 and
			.reduce(AsyncPredicate.from(exchange -> true), AsyncPredicate::and);
	}

	@SuppressWarnings("unchecked")
	private AsyncPredicate<ServerWebExchange> lookup(RouteDefinition route, PredicateDefinition predicate) {
		// 根据名字找对应的工厂, 根据 predicate 名称获取对应的 predicate factory
		RoutePredicateFactory<Object> factory = this.predicates.get(predicate.getName());
		if (factory == null) {
			throw new IllegalArgumentException("Unable to find RoutePredicateFactory with name " + predicate.getName());
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RouteDefinition " + route.getId() + " applying " + predicate.getArgs() + " to "
					+ predicate.getName());
		}


		/**
		 * 根据配置的参数 生成工厂里面的 配置对象
		 * 生成 PredicateArgsEvent 事件, 在 bind 里面 发事件
		 *
		 * 每个 RoutePredicateFactory 实现中都有Config，可以理解为我们配置的参数规则，生成此Config
		 */
		// @formatter:off
		Object config = this.configurationService.with(factory)
				.name(predicate.getName())
				.properties(predicate.getArgs())
				/**
				 * 发布事件
				 * {@link WeightCalculatorWebFilter#onApplicationEvent( ApplicationEvent)} 监听了这个事件
				 */
				.eventFunction((bound, properties) -> new PredicateArgsEvent(
						RouteDefinitionRouteLocator.this, route.getId(), properties))
				.bind();
		// @formatter:on

		// 将 config 作参数代入，调用 factory 的 applyAsync 方法创建 AsyncPredicate 对象
		return factory.applyAsync(config);
	}

}
