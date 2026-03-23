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

package org.springframework.cloud.gateway.discovery;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.gateway.filter.FilterDefinition;
import org.springframework.cloud.gateway.handler.predicate.PredicateDefinition;
import org.springframework.core.style.ToStringCreator;

/**
 * DiscoveryLocatorProperties与GatewayProperties类似用于读取discovery相关的配置，
 * 通过 DiscoveryLocatorProperties 装配DiscoveryClientRouteDefinitionLocator，DiscoveryClientRouteDefinitionLocator是RouteDefinitionLocator的子类，
 * 也是用来存放RouteDefinition的，最终会同PropertiesRouteDefinitionLocator一样被组合到CompositeRouteDefinitionLocator中
 *
 *
 * spring.cloud.gateway.server.webflux.discovery.locator.enabled 开启后,自动为注册中心所有服务创建路由
 */
@ConfigurationProperties("spring.cloud.gateway.server.webflux.discovery.locator")
public class DiscoveryLocatorProperties {

	/** Flag that enables DiscoveryClient gateway integration. */
	// 开启标识，默认关闭
	private boolean enabled = false;

	/**
	 * The prefix for the routeId, defaults to discoveryClient.getClass().getSimpleName()
	 * + "_". Service Id will be appended to create the routeId.
	 *
	 * 路由ID前缀，
	 * 默认为DiscoveryClient的类名称
	 * {@link org.springframework.cloud.client.discovery.DiscoveryClient}
	 * {@link com.alibaba.cloud.nacos.discovery.reactive.NacosReactiveDiscoveryClient}
	 *
	 * 默认：NacosReactiveDiscoveryClient_
	 *
	 * 是在 {@link DiscoveryClientRouteDefinitionLocator#DiscoveryClientRouteDefinitionLocator(String, DiscoveryLocatorProperties)} 处理的
	 */
	private String routeIdPrefix;

	/**
	 * SpEL expression that will evaluate whether to include a service in gateway
	 * integration or not, defaults to: true.
	 *
	 * 在这里有使用 {@link DiscoveryClientRouteDefinitionLocator#getRouteDefinitions()}
	 * 可用的变量来自 {@link ServiceInstance}
	 *
	 * 是否使用SpEL表达式, 默认包含所有服务
	 * # 只为服务名以 "api-" 开头的服务创建路由
	 * include-expression: "serviceId.startsWith('api-')"
	 *
	 * # 只为特定元数据的服务创建路由
	 * # include-expression: "metadata['gateway-enabled'] == 'true'"
	 *
	 * # 排除某些服务
	 * # include-expression: "!serviceId.contains('internal')"
	 */
	private String includeExpression = "true";

	/**
	 * SpEL expression that create the uri for each route, defaults to: 'lb://'+serviceId.
	 *
	 * 用来创建路由Route的uri表达式，最终会被解析为类似uri=lb://user-service，可覆盖
	 */
	private String urlExpression = "'lb://'+serviceId";

	/**
	 * Option to lower case serviceId in predicates and filters, defaults to false. Useful
	 * with eureka when it automatically uppercases serviceId. so MYSERIVCE, would match
	 * /myservice/**
	 *
	 * 将服务 ID 转为小写，用于匹配 URL 路径
	 * /USER-SERVICE/** → /user-service/**
	 */
	private boolean lowerCaseServiceId = false;

	/**
	 * 所有自动生成的路由添加统一的断言规则
	 * 由 {@link GatewayDiscoveryClientAutoConfiguration#initPredicates()} 初始化
	 *
	 * 可以自定义覆盖这个
	 */
	private List<PredicateDefinition> predicates = new ArrayList<>();

	/**
	 *
	 * 由 {@link GatewayDiscoveryClientAutoConfiguration#initFilters()} 初始化
	 *
	 */
	private List<FilterDefinition> filters = new ArrayList<>();

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public String getRouteIdPrefix() {
		return routeIdPrefix;
	}

	public void setRouteIdPrefix(String routeIdPrefix) {
		this.routeIdPrefix = routeIdPrefix;
	}

	public String getIncludeExpression() {
		return includeExpression;
	}

	public void setIncludeExpression(String includeExpression) {
		this.includeExpression = includeExpression;
	}

	public String getUrlExpression() {
		return urlExpression;
	}

	public void setUrlExpression(String urlExpression) {
		this.urlExpression = urlExpression;
	}

	public boolean isLowerCaseServiceId() {
		return lowerCaseServiceId;
	}

	public void setLowerCaseServiceId(boolean lowerCaseServiceId) {
		this.lowerCaseServiceId = lowerCaseServiceId;
	}

	public List<PredicateDefinition> getPredicates() {
		return predicates;
	}

	public void setPredicates(List<PredicateDefinition> predicates) {
		this.predicates = predicates;
	}

	public List<FilterDefinition> getFilters() {
		return filters;
	}

	public void setFilters(List<FilterDefinition> filters) {
		this.filters = filters;
	}

	@Override
	public String toString() {
		return new ToStringCreator(this).append("enabled", enabled)
			.append("routeIdPrefix", routeIdPrefix)
			.append("includeExpression", includeExpression)
			.append("urlExpression", urlExpression)
			.append("lowerCaseServiceId", lowerCaseServiceId)
			.append("predicates", predicates)
			.append("filters", filters)
			.toString();
	}

}
