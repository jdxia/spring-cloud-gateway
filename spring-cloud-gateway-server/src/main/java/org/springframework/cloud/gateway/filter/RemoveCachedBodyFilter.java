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

import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.web.server.ServerWebExchange;

// 清除上下文中的body缓存。是配合AdaptCachedBodyGlobalFilter使用的，AdaptCachedBodyGlobalFilter会将请求body放入缓存，等所有的filter执行完后再从上下文中将body缓存删除。
public class RemoveCachedBodyFilter implements GlobalFilter, Ordered {

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		/**
		 * {@link AdaptCachedBodyGlobalFilter} 这个是缓存
		 *
		 * 这个意思就是先调用下一个过滤器, 然后最后再执行这个
		 * 无论结果如何，当整个流结束时，一定会执行
		 */

		//调用下一个filter
		return chain.filter(exchange)
				//doFinally表示最终执行的操作
				.doFinally(s ->

				// 移除缓存的请求体
				ServerWebExchangeUtils.clearCachedRequestBody(exchange));
	}

	@Override
	public int getOrder() {
		return HIGHEST_PRECEDENCE;
	}

}
