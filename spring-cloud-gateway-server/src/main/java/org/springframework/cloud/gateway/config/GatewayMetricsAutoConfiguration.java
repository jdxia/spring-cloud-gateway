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

package org.springframework.cloud.gateway.config;

import java.util.List;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.observation.ObservationAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.tracing.TracingProperties;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.web.reactive.HttpHandlerAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.gateway.filter.GatewayMetricsFilter;
import org.springframework.cloud.gateway.filter.headers.observation.GatewayObservationConvention;
import org.springframework.cloud.gateway.filter.headers.observation.GatewayPropagatingSenderTracingObservationHandler;
import org.springframework.cloud.gateway.filter.headers.observation.ObservationClosingWebExceptionHandler;
import org.springframework.cloud.gateway.filter.headers.observation.ObservedRequestHttpHeadersFilter;
import org.springframework.cloud.gateway.filter.headers.observation.ObservedResponseHttpHeadersFilter;
import org.springframework.cloud.gateway.route.RouteDefinitionLocator;
import org.springframework.cloud.gateway.route.RouteDefinitionMetrics;
import org.springframework.cloud.gateway.support.tagsprovider.GatewayHttpTagsProvider;
import org.springframework.cloud.gateway.support.tagsprovider.GatewayPathTagsProvider;
import org.springframework.cloud.gateway.support.tagsprovider.GatewayRouteTagsProvider;
import org.springframework.cloud.gateway.support.tagsprovider.GatewayTagsProvider;
import org.springframework.cloud.gateway.support.tagsprovider.PropertiesTagsProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.web.reactive.DispatcherHandler;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = GatewayProperties.PREFIX + ".enabled", matchIfMissing = true)
@EnableConfigurationProperties(GatewayMetricsProperties.class)
// 在 WebFlux 的 HttpHandler 装配之前执行
@AutoConfigureBefore(HttpHandlerAutoConfiguration.class)
// 在 Micrometer 的 MeterRegistry、ObservationRegistry 装配之后
@AutoConfigureAfter({ MetricsAutoConfiguration.class, CompositeMeterRegistryAutoConfiguration.class,
		ObservationAutoConfiguration.class })
@ConditionalOnClass({ DispatcherHandler.class, MeterRegistry.class, MetricsAutoConfiguration.class })
public class GatewayMetricsAutoConfiguration {

	/**
	 *  Spring Boot 启动
	 *    │  读 META-INF/spring/...AutoConfiguration.imports (登记了 GatewayMetricsAutoConfiguration)
	 *    ▼
	 *  GatewayMetricsAutoConfiguration  ←判定 6 个注解：@ConditionalOnClass(有 webflux+micrometer+actuator?)
	 *    │                                            @ConditionalOnProperty(...enabled, 默认开)
	 *    │                                            @AutoConfigureAfter(MeterRegistry 已就绪?)
	 *    │  装配出 ▼
	 *    ├─ GatewayHttpTagsProvider ──┐
	 *    ├─ GatewayRouteTagsProvider ─┤  这些 TagsProvider 被
	 *    ├─ PropertiesTagsProvider ───┤  收集成 List<GatewayTagsProvider>
	 *    ├─ (GatewayPathTagsProvider)─┘  注入给 ▼
	 *    ├─ GatewayMetricsFilter (GlobalFilter, order=0) ★传统 Metrics 链路核心
	 *    ├─ RouteDefinitionMetrics (监听路由刷新 → gauge spring.cloud.gateway.routes.count)
	 *    └─ ObservabilityConfiguration（若有 ObservationRegistry）★Observation 链路
	 *         ├─ ObservedRequestHttpHeadersFilter / ObservedResponseHttpHeadersFilter
	 *         ├─ ObservationClosingWebExceptionHandler
	 *         └─ (有 Tracer 时) GatewayPropagatingSenderTracingObservationHandler
	 *
	 *
	 *
	 */

	/**
	 * 作用初始化 {@link GatewayMetricsFilter}
	 */

	@Bean
	public GatewayHttpTagsProvider gatewayHttpTagsProvider() {
		return new GatewayHttpTagsProvider();
	}

	// TODO: remove support for the property `metrics.tags.path.enabled` in the next major
	@Bean
	@ConditionalOnExpression("'${" + GatewayProperties.PREFIX + ".metrics.tags.path.enabled:false}' == 'true' || '${"
			+ GatewayProperties.PREFIX + ".metrics.path-tags.enabled:false}' == 'true'")
	public GatewayPathTagsProvider gatewayPathTagsProvider() {
		return new GatewayPathTagsProvider();
	}

	@Bean
	public GatewayRouteTagsProvider gatewayRouteTagsProvider() {
		return new GatewayRouteTagsProvider();
	}

	@Bean
	public PropertiesTagsProvider propertiesTagsProvider(GatewayMetricsProperties properties) {
		return new PropertiesTagsProvider(properties.getTags());
	}

	@Bean
	@ConditionalOnBean(MeterRegistry.class)
	@ConditionalOnProperty(name = GatewayProperties.PREFIX + ".metrics.enabled", matchIfMissing = true)
	// don't use @ConditionalOnEnabledGlobalFilter as the above property may
	// encompass more than just the filter
	/**
	 * {@link GatewayTagsProvider} 标签提供器
	 * 内置了4个
	 * {@link GatewayHttpTagsProvider} ,{@link GatewayRouteTagsProvider} , {@link GatewayPathTagsProvider}, {@link PropertiesTagsProvider}
	 * 扩展方式（官方推荐路径）：你只要往容器里再扔一个 @Bean 实现 GatewayTagsProvider，它就会被自动收集进来
	 * 这里有个高基数陷阱（生产事故高发区）：tenant 如果取值是无限的（比如 userId、traceId、完整 URL），会让 Micrometer / Prometheus 的时间序列（time series）爆炸，内存和存储被打穿。
	 * Tag 的取值必须是有限低基数集合。X-Tenant-Id 几百个租户 OK；userId 几百万就是灾难。
	 */
	public GatewayMetricsFilter gatewayMetricFilter(MeterRegistry meterRegistry,
			List<GatewayTagsProvider> tagsProviders, GatewayMetricsProperties properties) {
		return new GatewayMetricsFilter(meterRegistry, tagsProviders, properties.getPrefix());
	}

	@Bean
	@ConditionalOnBean(MeterRegistry.class)
	@ConditionalOnProperty(name = GatewayProperties.PREFIX + ".metrics.enabled", matchIfMissing = true)
	public RouteDefinitionMetrics routeDefinitionMetrics(MeterRegistry meterRegistry,
			RouteDefinitionLocator routeDefinitionLocator, GatewayMetricsProperties properties) {
		return new RouteDefinitionMetrics(meterRegistry, routeDefinitionLocator, properties.getPrefix());
	}

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnBean(ObservationRegistry.class)
	@ConditionalOnProperty(name = GatewayProperties.PREFIX + ".observability.enabled", matchIfMissing = true)
	static class ObservabilityConfiguration {

		@Bean
		@ConditionalOnMissingBean
		ObservedRequestHttpHeadersFilter observedRequestHttpHeadersFilter(ObservationRegistry observationRegistry,
				ObjectProvider<GatewayObservationConvention> gatewayObservationConvention) {
			return new ObservedRequestHttpHeadersFilter(observationRegistry,
					gatewayObservationConvention.getIfAvailable(() -> null));
		}

		@Bean
		@ConditionalOnMissingBean
		ObservedResponseHttpHeadersFilter observedResponseHttpHeadersFilter() {
			return new ObservedResponseHttpHeadersFilter();
		}

		@Bean
		@Order(Ordered.HIGHEST_PRECEDENCE)
		ObservationClosingWebExceptionHandler observationClosingWebExceptionHandler() {
			return new ObservationClosingWebExceptionHandler();
		}

		@Configuration(proxyBeanMethods = false)
		@ConditionalOnClass(Tracer.class)
		@ConditionalOnBean(Tracer.class)
		static class GatewayTracingConfiguration {

			@Bean
			@ConditionalOnMissingBean
			@ConditionalOnBean({ Propagator.class, TracingProperties.class })
			@Order(Ordered.HIGHEST_PRECEDENCE + 5)
			GatewayPropagatingSenderTracingObservationHandler gatewayPropagatingSenderTracingObservationHandler(
					Tracer tracer, Propagator propagator, TracingProperties tracingProperties) {
				return new GatewayPropagatingSenderTracingObservationHandler(tracer, propagator,
						tracingProperties.getBaggage().getRemoteFields());
			}

		}

	}

}
