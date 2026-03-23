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

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.discovery.ReactiveDiscoveryClient;
import org.springframework.cloud.client.discovery.composite.CompositeDiscoveryClientAutoConfiguration;
import org.springframework.cloud.gateway.config.GatewayAutoConfiguration;
import org.springframework.cloud.gateway.filter.FilterDefinition;
import org.springframework.cloud.gateway.filter.factory.RewritePathGatewayFilterFactory;
import org.springframework.cloud.gateway.handler.predicate.PathRoutePredicateFactory;
import org.springframework.cloud.gateway.handler.predicate.PredicateDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.DispatcherHandler;

import static org.springframework.cloud.gateway.filter.factory.RewritePathGatewayFilterFactory.REGEXP_KEY;
import static org.springframework.cloud.gateway.filter.factory.RewritePathGatewayFilterFactory.REPLACEMENT_KEY;
import static org.springframework.cloud.gateway.handler.predicate.RoutePredicateFactory.PATTERN_KEY;
import static org.springframework.cloud.gateway.support.NameUtils.normalizeFilterFactoryName;
import static org.springframework.cloud.gateway.support.NameUtils.normalizeRoutePredicateName;

/**
 * @author Spencer Gibb
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "spring.cloud.gateway.server.webflux.enabled", matchIfMissing = true)
@AutoConfigureBefore(GatewayAutoConfiguration.class)
@AutoConfigureAfter(CompositeDiscoveryClientAutoConfiguration.class)
@ConditionalOnClass({ DispatcherHandler.class, CompositeDiscoveryClientAutoConfiguration.class })
@EnableConfigurationProperties
public class GatewayDiscoveryClientAutoConfiguration {

	/**
	 * spring:
	 *     cloud:
	 *       gateway:
	 *         server:
	 *           webflux:
	 *             discovery:
	 *               locator:
	 *                 predicates:
	 *                   # 覆盖默认的 Path 断言
	 *                   - name: Path
	 *                     args:
	 *                       pattern: "'/api/'+serviceId+'/**'"  # /api/user-service/**
	 *
	 *                   # 添加额外断言
	 *                   - name: Method
	 *                     args:
	 *                       methods: "'GET,POST'"
	 *
	 *  会在这个里面 {@link GatewayDiscoveryClientAutoConfiguration#discoveryLocatorProperties()} 添加进去
	 *
	 */
	public static List<PredicateDefinition> initPredicates() {
		ArrayList<PredicateDefinition> definitions = new ArrayList<>();
		// TODO: add a predicate that matches the url at /serviceId?

		// add a predicate that matches the url at /serviceId/**
		PredicateDefinition predicate = new PredicateDefinition();
		//设置Predicate名称，Path，DiscoveryRouteDefinition 会使用 PathRoutePredicateFactory
		predicate.setName(normalizeRoutePredicateName(PathRoutePredicateFactory.class));

		/**'
		 * 设置Path参数，
		 * serviceId会在 {@link DiscoveryClientRouteDefinitionLocator#getRouteDefinitions()} 中替换为注册中心上的服务名，例如user-service
		 *
		 * 核心是 DiscoveryClientRouteDefinitionLocator 主要工作是获取到所有的注册中心上的服务实例，
		 * 根据服务信息创建 PredicateDefinition -> FilterDefinition -> RouteDefinition
		 * 供 CompositeRouteDefinitionLocator 获取
		 */
		predicate.addArg(PATTERN_KEY, "'/'+serviceId+'/**'");
		definitions.add(predicate);
		return definitions;
	}

	/**
	 * 为所有自动生成的路由添加统一的过滤器
	 * 请求 URL: /user-service/api/users/1
	 *            ↓ RewritePath
	 *   转发到后端: /api/users/1  (去掉了 /user-service 前缀)
	 *
	 * spring:
	 *     cloud:
	 *       gateway:
	 *         server:
	 *           webflux:
	 *             discovery:
	 *               locator:
	 *                 filters:
	 *                   # 保留默认的 RewritePath
	 *                   - name: RewritePath
	 *                     args:
	 *                       regexp: "'/' + serviceId + '/?(?<remaining>.*)'"
	 *                       replacement: "'/${remaining}'"
	 *
	 *                   # 添加额外过滤器
	 *                   - name: AddRequestHeader
	 *                     args:
	 *                       name: "'X-Service-Source'"
	 *                       value: "'gateway'"
	 *
	 *                   # 添加请求耗时统计
	 *                   - name: AddRequestHeader
	 *                     args:
	 *                       name: "'X-Request-Start'"
	 *                       value: "T(java.lang.System).currentTimeMillis()"
	 */
	public static List<FilterDefinition> initFilters() {
		ArrayList<FilterDefinition> definitions = new ArrayList<>();

		// add a filter that removes /serviceId by default
		FilterDefinition filter = new FilterDefinition();
		//设置使用的过滤器，此处使用RewritePathGatewayFilterFactory，因为后边会重写请求Path
		filter.setName(normalizeFilterFactoryName(RewritePathGatewayFilterFactory.class));

		/**
		 * 同Predicate，
		 * 会在 {@link DiscoveryClientRouteDefinitionLocator#getRouteDefinitions()}
		 * 将'service-id'替换为注册中心上的服务名，例如 /user-service/(?<remaining>.*)
		 */
		String regex = "'/' + serviceId + '/?(?<remaining>.*)'";
		String replacement = "'/${remaining}'";
		filter.addArg(REGEXP_KEY, regex);
		filter.addArg(REPLACEMENT_KEY, replacement);
		definitions.add(filter);

		return definitions;
	}

	@Bean
	public DiscoveryLocatorProperties discoveryLocatorProperties() {
		DiscoveryLocatorProperties properties = new DiscoveryLocatorProperties();
		//设置Predicate
		properties.setPredicates(initPredicates());
		//设置GatewayFilter
		properties.setFilters(initFilters());
		return properties;
	}

	/**
	 * 结合注册中心其实有两种DiscoveryClient使用，一种是原始的DiscoveryClient，一种是ReactiveDiscoveryClient，
	 * 不同的注册中心都有相应的实现，
	 * 如nacos的 NacosReactiveDiscoveryClient。可以通过配置spring.cloud.discovery.reactive.enabled=true来开启使用Reactive模式的
	 */
	@Configuration(proxyBeanMethods = false)
	@ConditionalOnProperty(value = "spring.cloud.discovery.reactive.enabled", matchIfMissing = true)
	public static class ReactiveDiscoveryClientRouteDefinitionLocatorConfiguration {

		/**
		 *
		 * @param discoveryClient Reactive的实现，如果使用nacos，
		 * 这里注入的为 {@link com.alibaba.cloud.nacos.discovery.reactive.NacosReactiveDiscoveryClient}
		 */
		@Bean
		@ConditionalOnProperty(name = "spring.cloud.gateway.server.webflux.discovery.locator.enabled")
		public DiscoveryClientRouteDefinitionLocator discoveryClientRouteDefinitionLocator(
				ReactiveDiscoveryClient discoveryClient, DiscoveryLocatorProperties properties) {

			// 构造函数执行, 此时就已经开始订阅注册中心的服务了
			return new DiscoveryClientRouteDefinitionLocator(discoveryClient, properties);
		}

	}

}
