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

package org.springframework.cloud.gateway.filter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.gateway.event.PredicateArgsEvent;
import org.springframework.cloud.gateway.event.RefreshRoutesEvent;
import org.springframework.cloud.gateway.event.RouteDeletedEvent;
import org.springframework.cloud.gateway.event.WeightDefinedEvent;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.support.ConfigurationService;
import org.springframework.cloud.gateway.support.WeightConfig;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.event.SmartApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.core.log.LogMessage;
import org.springframework.core.style.ToStringCreator;
import org.springframework.util.Assert;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;

import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.WEIGHT_ATTR;

/**
 * @author Spencer Gibb
 * @author Alexey Nakidkin
 *
 * 这是一个 WebFilter , 先走 WebFilter 再走 dispatch
 */
public class WeightCalculatorWebFilter implements WebFilter, Ordered, SmartApplicationListener {

	/**
	 * Order of Weight Calculator Web filter.
	 */
	public static final int WEIGHT_CALC_FILTER_ORDER = 10001;

	private static final Log log = LogFactory.getLog(WeightCalculatorWebFilter.class);

	private final ObjectProvider<RouteLocator> routeLocator;

	private final ConfigurationService configurationService;

	private Function<ServerWebExchange, Double> randomFunction = exchange -> ThreadLocalRandom.current().nextDouble();

	private int order = WEIGHT_CALC_FILTER_ORDER;

	private Map<String, GroupWeightConfig> groupWeights = new ConcurrentHashMap<>();

	private final AtomicBoolean routeLocatorInitialized = new AtomicBoolean();

	public WeightCalculatorWebFilter(ObjectProvider<RouteLocator> routeLocator,
			ConfigurationService configurationService) {
		this.routeLocator = routeLocator;
		this.configurationService = configurationService;
	}

	/* for testing */
	static Map<String, String> getWeights(ServerWebExchange exchange) {
		Map<String, String> weights = exchange.getAttribute(WEIGHT_ATTR);

		if (weights == null) {
			weights = new ConcurrentHashMap<>();
			exchange.getAttributes().put(WEIGHT_ATTR, weights);
		}
		return weights;
	}

	@Override
	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	@Deprecated
	public void setRandom(Random random) {
		Assert.notNull(random, "random may not be null");
		this.randomFunction = exchange -> random.nextDouble();
	}

	public void setRandomSupplier(Supplier<Double> randomSupplier) {
		Assert.notNull(randomSupplier, "randomSupplier may not be null");
		this.randomFunction = exchange -> randomSupplier.get();
	}

	public void setRandomFunction(Function<ServerWebExchange, Double> randomFunction) {
		Assert.notNull(randomFunction, "randomFunction may not be null");
		this.randomFunction = randomFunction;
	}

	@Override
	public boolean supportsEventType(Class<? extends ApplicationEvent> eventType) {
		// from config file
		return PredicateArgsEvent.class.isAssignableFrom(eventType) ||
		// from java dsl
				WeightDefinedEvent.class.isAssignableFrom(eventType) ||
				// from actuator or custom call
				RouteDeletedEvent.class.isAssignableFrom(eventType) ||
				// force initialization
				RefreshRoutesEvent.class.isAssignableFrom(eventType);
	}

	@Override
	public boolean supportsSourceType(Class<?> sourceType) {
		return true;
	}

	@Override
	public void onApplicationEvent(ApplicationEvent event) {
		// 匹配器参数的事件, 可能有多次
		if (event instanceof PredicateArgsEvent) {
			// 监听到 RouteDefinitionRouteLocator 发布的事件
			handle((PredicateArgsEvent) event);
		}
		else if (event instanceof WeightDefinedEvent) {
			addWeightConfig(((WeightDefinedEvent) event).getWeightConfig());
		}
		else if (event instanceof RouteDeletedEvent) {
			removeWeightConfig(((RouteDeletedEvent) event).getRouteId());
		}
		else if (event instanceof RefreshRoutesEvent && routeLocator != null) {
			// forces initialization
			if (routeLocatorInitialized.compareAndSet(false, true)) {
				// on first time, block so that app fails to start if there are errors in
				// routes
				// see gh-1574
				routeLocator.ifAvailable(locator -> locator.getRoutes().blockLast());
			}
			else {
				// this preserves previous behaviour on refresh, this could likely go away
				routeLocator.ifAvailable(locator -> locator.getRoutes().subscribe());
			}
		}

	}

	public void handle(PredicateArgsEvent event) {
		Map<String, Object> args = event.getArgs();

		if (args.isEmpty() || !hasRelevantKey(args)) {
			return;
		}

		WeightConfig config = new WeightConfig(event.getRouteId());

		// 获取到当前路由的配置信息
		this.configurationService.with(config).name(WeightConfig.CONFIG_PREFIX).normalizedProperties(args).bind();

		// 往下
		addWeightConfig(config);
	}

	private boolean hasRelevantKey(Map<String, Object> args) {
		return args.keySet().stream().anyMatch(key -> key.startsWith(WeightConfig.CONFIG_PREFIX + "."));
	}

	/* for testing */ void addWeightConfig(WeightConfig weightConfig) {
		//获取当前路由的group
		String group = weightConfig.getGroup();
		GroupWeightConfig config;
		// only create new GroupWeightConfig rather than modify
		// and put at end of calculations. This avoids concurency problems
		// later during filter execution.
		// 判断 groupWeights 是否包含了已经包含了同group的权重配置，此处的groupWeights是所有的路由权重信息
		if (groupWeights.containsKey(group)) {
			//如果有也创建一个信息，并将前边的该group下路由权重信息初始化进去
			config = new GroupWeightConfig(groupWeights.get(group));
		}
		else {
			config = new GroupWeightConfig(group);
		}

		//添加当前路由+路由权重
		config.weights.put(weightConfig.getRouteId(), weightConfig.getWeight());

		// recalculate

		// normalize weights
		int weightsSum = 0;

		/**
		 * 计算配置的所有的路由权重和
		 * 假设，我们配置rout1、route2、route3三个路由的权重分别为2,7,1，那么weightSum计算后为10
		 */
		for (Integer weight : config.weights.values()) {
			weightsSum += weight;
		}

		final AtomicInteger index = new AtomicInteger(0);

		//遍历
		for (Map.Entry<String, Integer> entry : config.weights.entrySet()) {
			//获取到路由ID
			String routeId = entry.getKey();

			//获取到路由的权重
			Integer weight = entry.getValue();

			//计算出当前路由的权重占比
			//rout1：0.2，route2：0.7，route3：0.1
			Double nomalizedWeight = weight / (double) weightsSum;

			//放入normalizedWeights
			config.normalizedWeights.put(routeId, nomalizedWeight);

			// recalculate rangeIndexes
			config.rangeIndexes.put(index.getAndIncrement(), routeId);
		}

		// TODO: calculate ranges
		config.ranges.clear();

		//放入0号位置数0.0
		config.ranges.add(0.0);

		List<Double> values = new ArrayList<>(config.normalizedWeights.values());
		for (int i = 0; i < values.size(); i++) {
			Double currentWeight = values.get(i);
			Double previousRange = config.ranges.get(i);
			Double range = previousRange + currentWeight;
			config.ranges.add(range);
		}

		//ranges ：大约为 0.0, 0.2, 0.9, 1.0
		//相邻两个index之间代表的是一个路由的范围，
		//如rout1：0.2，route2：0.7，route3：0.1  那ranges的元素为0.0, 0.2, 0.9, 1.0
		//0.0到0.2则表示route1的权重范围，0.2到0.9表示的route2的权重范围以此类推
		if (log.isTraceEnabled()) {
			log.trace("Recalculated group weight config " + config);
		}
		// only update after all calculations
		//添加权重分组，key：分组 value：分组下的所有路由的权重信息
		groupWeights.put(group, config);
	}

	private void removeWeightConfig(String routeId) {
		log.trace(LogMessage.format("Removing weight config for route %s", routeId));
		groupWeights.forEach((group, weightConfig) -> {
			if (weightConfig.normalizedWeights.containsKey(routeId)) {
				weightConfig.normalizedWeights.remove(routeId);
				weightConfig.weights.remove(routeId);
			}
		});
	}

	/* for testing */ Map<String, GroupWeightConfig> getGroupWeights() {
		return groupWeights;
	}

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
		Map<String, String> weights = getWeights(exchange);

		for (String group : groupWeights.keySet()) {
			//获取到当前分组的所有路由及权重信息
			GroupWeightConfig config = groupWeights.get(group);

			if (config == null) {
				if (log.isDebugEnabled()) {
					log.debug("No GroupWeightConfig found for group: " + group);
				}
				continue; // nothing we can do, but this is odd
			}

			// Usually, multiple threads accessing the same random object will have some
			// performance problems, so we can use ThreadLocalRandom by default
			//生成随机数
			double r = randomFunction.apply(exchange);

			//获取到当前分组的所有路由的权重范围
			List<Double> ranges = config.ranges;

			if (log.isTraceEnabled()) {
				log.trace("Weight for group: " + group + ", ranges: " + ranges + ", r: " + r);
			}

			// 看落在那个区间, 走那个 routeId
			for (int i = 0; i < ranges.size() - 1; i++) {
				//如果生成的随机数大于等于当前的元素，并且小于下一元素，说明属于当前路由，则获取到路由ID放入weights中返回
				//WeightRoutePredicateFactory只需要判断weights中是否有当前路由的group，
				//如果有，则进一步判断当前路由ID是否为这里计算出来的路由id即可
				if (r >= ranges.get(i) && r < ranges.get(i + 1)) {
					String routeId = config.rangeIndexes.get(i);
					weights.put(group, routeId);
					break;
				}
			}
		}

		if (log.isTraceEnabled()) {
			log.trace("Weights attr: " + weights);
		}

		return chain.filter(exchange);
	}

	/* for testing */ static class GroupWeightConfig {

		String group;

		//key：路由ID  value：权重
		LinkedHashMap<String, Integer> weights = new LinkedHashMap<>();

		//路由的权重占比
		LinkedHashMap<String, Double> normalizedWeights = new LinkedHashMap<>();

		LinkedHashMap<Integer, String> rangeIndexes = new LinkedHashMap<>();

		//相邻两个index之间代表的是一个路由的范围，
		//如rout1：0.2，route2：0.7，route3：0.1  那ranges的元素为0.0, 0.2, 0.9, 1.0
		//0.0到0.2则表示route1的权重范围，0.2到0.9表示的route2的权重范围以此类推
		List<Double> ranges = new ArrayList<>();

		GroupWeightConfig(String group) {
			this.group = group;
		}

		GroupWeightConfig(GroupWeightConfig other) {
			this.group = other.group;
			this.weights = new LinkedHashMap<>(other.weights);
			this.normalizedWeights = new LinkedHashMap<>(other.normalizedWeights);
			this.rangeIndexes = new LinkedHashMap<>(other.rangeIndexes);
		}

		@Override
		public String toString() {
			return new ToStringCreator(this).append("group", group)
				.append("weights", weights)
				.append("normalizedWeights", normalizedWeights)
				.append("rangeIndexes", rangeIndexes)
				.toString();
		}

	}

}
