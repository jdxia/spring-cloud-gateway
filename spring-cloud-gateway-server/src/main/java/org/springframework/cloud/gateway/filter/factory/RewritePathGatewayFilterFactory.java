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

package org.springframework.cloud.gateway.filter.factory;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.util.Assert;
import org.springframework.web.server.ServerWebExchange;

import static org.springframework.cloud.gateway.support.GatewayToStringStyler.filterToStringCreator;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.addOriginalRequestUrl;

/**
 * @author Spencer Gibb
 *
 * 当结合注册中心时SCG会为每个路由添加PathRoutePredicateFactory
 * 和RewritePathGatewayFilterFactory。PathRoutePredicateFactory用来计算请求是否符合当前路由的条件，
 * RewritePathGatewayFilterFactory用来重写请求Path，
 * 参数regexp=/user-service/(?<remaining>.*)，replacement=$(remaining)，
 * 例如请求的Path为/user-service/api/hello，会被重写为/api/hello
 */
public class RewritePathGatewayFilterFactory
		extends AbstractGatewayFilterFactory<RewritePathGatewayFilterFactory.Config> {

	/**
	 * Regexp key.
	 */
	public static final String REGEXP_KEY = "regexp";

	/**
	 * Replacement key.
	 */
	public static final String REPLACEMENT_KEY = "replacement";

	public RewritePathGatewayFilterFactory() {
		super(Config.class);
	}

	@Override
	public List<String> shortcutFieldOrder() {
		return Arrays.asList(REGEXP_KEY, REPLACEMENT_KEY);
	}

	/**
	 * 注意，$\ 用于替代 $ ，避免和 YAML 语法冲突
	 * filters:
	 *   - RewritePath=/foo/(?<segment>.*), /$\{segment}
	 *
	 *
	 */
	@Override
	public GatewayFilter apply(Config config) {
		// `$\` 用于替代 `$` ，避免和 YAML 语法冲突
		String replacement = config.replacement.replace("$\\", "$");
		Pattern pattern = Pattern.compile(config.regexp);
		return new GatewayFilter() {
			@Override
			public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
				ServerHttpRequest req = exchange.getRequest();

				// 添加 原始请求URI 到 GATEWAY_ORIGINAL_REQUEST_URL_ATTR
				//每次进行重写时，都在上下文中保留一次原址的请求URI
				addOriginalRequestUrl(exchange, req.getURI());

				// 重写 Path
				String path = req.getURI().getRawPath();

				//根据配置的正则进行替换
				// regexp=/user-service/(?<remaining>.*)，replacement=$(remaining)，例如请求的Path为/user-service/api/hello，会被重写为/api/hello。
				String newPath = pattern.matcher(path).replaceAll(replacement);

				// 创建新的 ServerHttpRequest
				//基于重写后的Path构建新的请求
				ServerHttpRequest request = req.mutate()
						// 设置 Path
						.path(newPath).build();

				// 添加 请求URI 到 GATEWAY_REQUEST_URL_ATTR
				//将新的请求URI放入上下文中，供后边的Filter使用
				exchange.getAttributes().put(GATEWAY_REQUEST_URL_ATTR, request.getURI());

				// 创建新的 ServerWebExchange ，提交过滤器链继续过滤
				return chain.filter(exchange.mutate().request(request).build());
			}

			@Override
			public String toString() {
				return filterToStringCreator(RewritePathGatewayFilterFactory.this)
					.append(config.getRegexp(), replacement)
					.toString();
			}
		};
	}

	public static class Config {

		private String regexp;

		private String replacement;

		public String getRegexp() {
			return regexp;
		}

		public Config setRegexp(String regexp) {
			Assert.hasText(regexp, "regexp must have a value");
			this.regexp = regexp;
			return this;
		}

		public String getReplacement() {
			return replacement;
		}

		public Config setReplacement(String replacement) {
			Assert.notNull(replacement, "replacement must not be null");
			this.replacement = replacement;
			return this;
		}

	}

}
