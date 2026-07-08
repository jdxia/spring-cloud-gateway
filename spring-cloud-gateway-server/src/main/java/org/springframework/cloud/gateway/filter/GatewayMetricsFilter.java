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

import java.util.List;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Sample;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.gateway.support.tagsprovider.GatewayHttpTagsProvider;
import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.support.tagsprovider.GatewayTagsProvider;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.server.ServerWebExchange;

/**
 * @author Tony Clarke
 * @author Ingyu Hwang
 *
 * 网关指标监控过滤器，需要添加spring-boot-starter-actuator依赖，
 * 可通过spring.cloud.gateway.metrics.enabled=true/false进行配置，默认为开启状态。
 * 可以通过/actuator/metrics/gateway.requests来访问查看。
 *
 * 可提供如下数据：
 *
 * routeId
 * routUrI
 * outcome：结果，按HttpStatus.Series分类。
 * 		INFORMATIONAL
 * 		SUCCESSFUL
 * 		REDIRECTION
 * 		CLIENT_ERROR
 * 		SERVER_ERROR
 * statue**：状态**
 * httpStatusCode：状态码
 * httpMethod：请求方式
 */
public class GatewayMetricsFilter implements GlobalFilter, Ordered {
	/**
	 * | 具体 Meter 类型 | 干什么的 | 生活类比 |
	 * | --- | --- | --- |
	 * | **`Timer`（计时器）** | 记录「一段操作耗时多久」+「发生了多少次」 | 跑步用的**秒表**，每跑一圈按一下，它记下总圈数和每圈用时 |
	 * | **`Counter`（计数器）** | 只增不减地数数：「这件事发生了几次」 | 进门的**计数闸机**，每过一人 +1，永不回退 |
	 * | **`Gauge`（仪表盘）** | 测「某个瞬间的当前值」，可上可下 | 汽车的**油量表 / 温度计**，此刻多少就是多少 |
	 *
	 * > - **Counter**：单调递增，问的是「累计发生了多少次」。比如「网站总访问量」。
	 * > - **Gauge**：随时上下浮动，问的是「此时此刻是多少」。比如「当前在线人数」「当前队列长度」。
	 * > 误用最常见的坑：用 Gauge 去记「累计请求数」——一重启就归零，曲线乱跳。
	 */

	private static final Log log = LogFactory.getLog(GatewayMetricsFilter.class);

	private final MeterRegistry meterRegistry;

	private GatewayTagsProvider compositeTagsProvider;

	private final String metricsPrefix;

	public GatewayMetricsFilter(MeterRegistry meterRegistry, List<GatewayTagsProvider> tagsProviders,
			String metricsPrefix) {
		this.meterRegistry = meterRegistry;
		this.compositeTagsProvider = tagsProviders.stream().reduce(exchange -> Tags.empty(), GatewayTagsProvider::and);

		// 去掉用户配置前缀末尾多余的 .
		if (metricsPrefix.endsWith(".")) {
			this.metricsPrefix = metricsPrefix.substring(0, metricsPrefix.length() - 1);
		}
		else {
			this.metricsPrefix = metricsPrefix;
		}
	}

	public String getMetricsPrefix() {
		return metricsPrefix;
	}

	@Override
	public int getOrder() {
		// start the timer as soon as possible and report the metric event before we write
		// response to client

		// -1 + 1 = 0
		return NettyWriteResponseFilter.WRITE_RESPONSE_FILTER_ORDER + 1;
	}

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		// 记录起点时间
		Sample sample = Timer.start(meterRegistry);

		// 放行，跑完下游
		return chain.filter(exchange)
				// 双保险：无论请求成功还是异常，计时都要收尾，不能漏。这是"指标不丢"的关键

				// 成功结算
			.doOnSuccess(aVoid -> endTimerRespectingCommit(exchange, sample))
				// 失败也结算
			.doOnError(throwable -> endTimerRespectingCommit(exchange, sample));
	}

	private void endTimerRespectingCommit(ServerWebExchange exchange, Sample sample) {

		ServerHttpResponse response = exchange.getResponse();
		if (response.isCommitted()) {
			// 真正产生指标
			endTimerInner(exchange, sample);
		}
		else {
			// 还没写，挂个"提交前"回调, 流式响应
			response.beforeCommit(() -> {
				endTimerInner(exchange, sample);
				return Mono.empty();
			});
		}
	}

	private void endTimerInner(ServerWebExchange exchange, Sample sample) {

		/**
		 *  此刻 response 状态已确定，能拿到真实
		 * "[tag(httpMethod=GET),tag(httpStatusCode=200),tag(outcome=SUCCESSFUL),tag(routeId=user_route),tag(routeUri=lb://user-demo),tag(status=OK)]"
		 *
		 * {@link GatewayHttpTagsProvider#apply(ServerWebExchange)}
		 * String outcome = "CUSTOM";  String status = "CUSTOM";  String httpStatusCodeStr = "NA";
		 * 三个兜底默认值——拿不到标准状态时就是这些
		 */
		Tags tags = compositeTagsProvider.apply(exchange);

		if (log.isTraceEnabled()) {
			log.trace(metricsPrefix + ".requests tags: " + tags);
		}

		/**
		 * Micrometer 按 (name + tags) 唯一确定一个 Timer（不存在则创建、存在则复用）
		 * 把 now - start 的耗时记进这个 Timer 的分布里
		 * name 是 spring.cloud.gateway.requests
		 */
		sample.stop(meterRegistry.timer(metricsPrefix + ".requests", tags));
	}

}
