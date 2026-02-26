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

package org.springframework.cloud.gateway.support;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import io.netty.buffer.Unpooled;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.gateway.filter.RouteToRequestUrlFilter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.gateway.filter.factory.GatewayFilterFactory;
import org.springframework.cloud.gateway.handler.AsyncPredicate;
import org.springframework.cloud.gateway.handler.predicate.RoutePredicateFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBuffer;
import org.springframework.core.io.buffer.NettyDataBuffer;
import org.springframework.core.io.buffer.PooledDataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.AbstractServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.DispatcherHandler;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;

/**
 * @author Spencer Gibb
 */
public final class ServerWebExchangeUtils {

	private static final Log log = LogFactory.getLog(ServerWebExchangeUtils.class);

	/**
	 * Preserve-Host header attribute name.
	 *
	 * 是否保留原始 Host 头, 存储的值类型：Boolean
	 * 某些后端服务（如基于虚拟主机的服务）依赖 Host 头来路由请求。如果不设置这个，Gateway 转发时会把 Host 改成下游服务的地址，导致后端路由失败
	 *
	 * filters:
	 *  - PreserveHostHeader
	 */
	public static final String PRESERVE_HOST_HEADER_ATTRIBUTE = qualify("preserveHostHeader");

	/**
	 * URI template variables attribute name.
	 */
	public static final String URI_TEMPLATE_VARIABLES_ATTRIBUTE = qualify("uriTemplateVariables");

	/**
	 * Client response attribute name.
	 *
	 * 下游服务的响应对象
	 */
	public static final String CLIENT_RESPONSE_ATTR = qualify("gatewayClientResponse");

	/**
	 * Client response connection attribute name.
	 *
	 * Netty 连接对象
	 */
	public static final String CLIENT_RESPONSE_CONN_ATTR = qualify("gatewayClientResponseConnection");

	/**
	 * Client response header names attribute name.
	 */
	public static final String CLIENT_RESPONSE_HEADER_NAMES = qualify("gatewayClientResponseHeaderNames");

	/**
	 * Gateway route attribute name.
	 *
	 * 当前匹配的路由, 核心属性, 存储的值类型：Route
	 * 找到第一个匹配的路由，然后把这个 Route 对象放进这里
	 *
	 * 很多 filter 都会读取这个
	 */
	public static final String GATEWAY_ROUTE_ATTR = qualify("gatewayRoute");

	/**
	 * Original Reactor Context corresponding to the processed request.
	 */
	public static final String GATEWAY_REACTOR_CONTEXT_ATTR = qualify("gatewayReactorContext");

	/**
	 * Gateway request URL attribute name.
	 *
	 * 最终转发的目标 URL, 存储的值类型：URI
	 * 把原始请求 URL 和路由定义的目标 URI 合并后 得到最终的转发目标 URL
	 * 由 {@link RouteToRequestUrlFilter} 设置, 他的order 是 ROUTE_TO_URL_FILTER_ORDER
	 *
	 * 几乎所有路由类Filter 都依赖它来决定往哪里发请求
	 */
	public static final String GATEWAY_REQUEST_URL_ATTR = qualify("gatewayRequestUrl");

	/**
	 * Gateway original request URL attribute name.
	 *
	 * 原始请求 URL 集合, 存储的值类型：LinkedHashSet<URI>
	 * 每当有Filter 修改了请求 URL（如 RewritePathGatewayFilterFactory、StripPrefixGatewayFilterFactory、PrefixPathGatewayFilterFactory 等），
	 *  都会先调用 addOriginalRequestUrl() 把修改前的 URL 保存到这个集合中
	 */
	public static final String GATEWAY_ORIGINAL_REQUEST_URL_ATTR = qualify("gatewayOriginalRequestUrl");

	/**
	 * Gateway handler mapper attribute name.
	 *
	 * 处理请求的 HandlerMapping
	 * 存储的值类型：String（HandlerMapping 的类名）
	 *
	 * 由 RoutePredicateHandlerMapping.getHandlerInternal() 设置，值为当前 HandlerMapping 的简单类名。主要用于调试和 metrics，让你知道请求是被哪个 HandlerMapping 处理的
	 */
	public static final String GATEWAY_HANDLER_MAPPER_ATTR = qualify("gatewayHandlerMapper");

	/**
	 * Gateway scheme prefix attribute name.
	 *
	 * 路由 URI 的 scheme 前缀
	 * 存储的值类型：String
	 * 由 RouteToRequestUrlFilter 设置。当路由配置使用了 lb:http:// 这种复合 scheme 时，lb 会被提取出来放到这个属性中：
	 */
	public static final String GATEWAY_SCHEME_PREFIX_ATTR = qualify("gatewaySchemePrefix");

	/**
	 * Gateway predicate route attribute name.
	 *
	 * 谓词匹配阶段的路由 ID
	 * 存储的值类型：String（路由 ID）
	 * 在 RoutePredicateHandlerMapping.lookupRoute() 中设置，在谓词匹配阶段把当前正在尝试匹配的路由 ID 放进去。PathRoutePredicateFactory 和 WeightRoutePredicateFactory 会读取它。
	 * 这个属性的作用是让Predicate 知道自己正在为哪个路由做匹配，这在 WeightRoutePredicateFactory 中尤其重要——它需要知道路由 ID 才能做权重计算
	 */
	public static final String GATEWAY_PREDICATE_ROUTE_ATTR = qualify("gatewayPredicateRouteAttr");

	/**
	 * Gateway predicate matched path attribute name.
	 *
	 * Path谓词匹配到的路径模式
	 * 存储的值类型：String（路径模式，如 /api/**）
	 * 由 PathRoutePredicateFactory 在路径匹配成功后设置，记录匹配到的路径模式。GatewayPathTagsProvider 用它来给 metrics 打 path 标签
	 */
	public static final String GATEWAY_PREDICATE_MATCHED_PATH_ATTR = qualify("gatewayPredicateMatchedPathAttr");

	/**
	 * Gateway predicate matched path route id attribute name.
	 *
	 * 匹配路径对应的路由 ID
	 */
	public static final String GATEWAY_PREDICATE_MATCHED_PATH_ROUTE_ID_ATTR = qualify(
			"gatewayPredicateMatchedPathRouteIdAttr");

	/**
	 * Gateway predicate path container attribute name.
	 *
	 * Path 谓词的路径容器
	 * 存储的值类型：PathContainer
	 * 由 PathRoutePredicateFactory 设置，存储解析后的请求路径
	 */
	public static final String GATEWAY_PREDICATE_PATH_CONTAINER_ATTR = qualify("gatewayPredicatePathContainer");

	/**
	 * Weight attribute name.
	 */
	public static final String WEIGHT_ATTR = qualify("routeWeight");

	/**
	 * Original response Content-Type attribute name.
	 */
	public static final String ORIGINAL_RESPONSE_CONTENT_TYPE_ATTR = "original_response_content_type";

	/**
	 * CircuitBreaker execution exception attribute name.
	 */
	public static final String CIRCUITBREAKER_EXECUTION_EXCEPTION_ATTR = qualify("circuitBreakerExecutionException");

	/**
	 * Used when a routing filter has been successfully called. Allows users to write
	 * custom routing filters that disable built in routing filters.
	 *
	 * 请求是否已被路由
	 * 存储的值类型：Boolean
	 * 这是一个防重复路由的标志位。当某个路由 Filter（如 NettyRoutingFilter、WebClientHttpRoutingFilter、ForwardRoutingFilter）成功处理了请求后，
	 * 会调用 setAlreadyRouted(exchange) 将其设为 true。
	 * 其他路由 Filter 在执行前会先调用 isAlreadyRouted(exchange) 检查，如果已经路由过了就直接跳过。这样可以避免请求被多个路由 Filter 重复转发
	 *
	 * 如果你写了自定义路由 Filter，必须在成功路由后调用 setAlreadyRouted()，否则内置的路由 Filter 还会再转发一次。
	 * 同样，如果你想实现重试逻辑，需要先调用 removeAlreadyRouted() 清除标志。RetryGatewayFilterFactory 的 reset() 方法就是这么做的。
	 */
	public static final String GATEWAY_ALREADY_ROUTED_ATTR = qualify("gatewayAlreadyRouted");

	/**
	 * Gateway already prefixed attribute name.
	 */
	public static final String GATEWAY_ALREADY_PREFIXED_ATTR = qualify("gatewayAlreadyPrefixed");

	/**
	 * Cached ServerHttpRequestDecorator attribute name. Used when
	 * {@link #cacheRequestBodyAndRequest(ServerWebExchange, Function)} is called.
	 *
	 * 缓存请求体
	 */
	public static final String CACHED_SERVER_HTTP_REQUEST_DECORATOR_ATTR = "cachedServerHttpRequestDecorator";

	/**
	 * Cached request body key. Used when
	 * {@link #cacheRequestBodyAndRequest(ServerWebExchange, Function)} or
	 * {@link #cacheRequestBody(ServerWebExchange, Function)} are called.
	 */
	public static final String CACHED_REQUEST_BODY_ATTR = "cachedRequestBody";

	/**
	 * Gateway LoadBalancer {@link Response} attribute name.
	 */
	public static final String GATEWAY_LOADBALANCER_RESPONSE_ATTR = qualify("gatewayLoadBalancerResponse");

	/**
	 * Gateway Client {@code Observation} attribute name.
	 */
	public static final String GATEWAY_OBSERVATION_ATTR = qualify("gateway.observation");

	private static final byte[] EMPTY_BYTES = {};

	private ServerWebExchangeUtils() {
		throw new AssertionError("Must not instantiate utility class.");
	}

	private static String qualify(String attr) {
		return ServerWebExchangeUtils.class.getName() + "." + attr;
	}

	public static void setAlreadyRouted(ServerWebExchange exchange) {
		exchange.getAttributes().put(GATEWAY_ALREADY_ROUTED_ATTR, true);
	}

	public static void removeAlreadyRouted(ServerWebExchange exchange) {
		exchange.getAttributes().remove(GATEWAY_ALREADY_ROUTED_ATTR);
	}

	public static boolean isAlreadyRouted(ServerWebExchange exchange) {
		return exchange.getAttributeOrDefault(GATEWAY_ALREADY_ROUTED_ATTR, false);
	}

	public static boolean setResponseStatus(ServerWebExchange exchange, HttpStatus httpStatus) {
		boolean response = exchange.getResponse().setStatusCode(httpStatus);
		if (!response && log.isWarnEnabled()) {
			log.warn("Unable to set status code to " + httpStatus + ". Response already committed.");
		}
		return response;
	}

	public static void reset(ServerWebExchange exchange) {
		// TODO: what else to do to reset exchange?
		Set<String> addedHeaders = exchange.getAttributeOrDefault(CLIENT_RESPONSE_HEADER_NAMES, Collections.emptySet());
		addedHeaders.forEach(header -> exchange.getResponse().getHeaders().remove(header));
		removeAlreadyRouted(exchange);
	}

	public static boolean setResponseStatus(ServerWebExchange exchange, HttpStatusHolder statusHolder) {
		if (exchange.getResponse().isCommitted()) {
			return false;
		}
		if (log.isDebugEnabled()) {
			log.debug("Setting response status to " + statusHolder);
		}
		if (statusHolder.getHttpStatus() != null) {
			return setResponseStatus(exchange, statusHolder.getHttpStatus());
		}
		if (statusHolder.getStatus() != null && exchange.getResponse() instanceof AbstractServerHttpResponse) { // non-standard
			((AbstractServerHttpResponse) exchange.getResponse()).setRawStatusCode(statusHolder.getStatus());
			return true;
		}
		return false;
	}

	public static boolean containsEncodedParts(URI uri) {
		boolean encoded = (uri.getRawQuery() != null && uri.getRawQuery().contains("%"))
				|| (uri.getRawPath() != null && uri.getRawPath().contains("%"));

		// Verify if it is really fully encoded. Treat partial encoded as unencoded.
		if (encoded) {
			try {
				UriComponentsBuilder.fromUri(uri).build(true);
				return true;
			}
			catch (IllegalArgumentException ignored) {
				if (log.isTraceEnabled()) {
					log.trace("Error in containsEncodedParts", ignored);
				}
			}

			return false;
		}

		return encoded;
	}

	public static MultiValueMap<String, String> encodeQueryParams(MultiValueMap<String, String> params) {
		MultiValueMap<String, String> encodedQueryParams = new LinkedMultiValueMap<>(params.size());
		for (Map.Entry<String, List<String>> entry : params.entrySet()) {
			for (String value : entry.getValue()) {
				encodedQueryParams.add(UriUtils.encode(entry.getKey(), StandardCharsets.UTF_8),
						UriUtils.encode(value, StandardCharsets.UTF_8));
			}
		}
		return CollectionUtils.unmodifiableMultiValueMap(encodedQueryParams);
	}

	public static HttpStatus parse(String statusString) {
		HttpStatus httpStatus;

		try {
			int status = Integer.parseInt(statusString);
			httpStatus = HttpStatus.resolve(status);
		}
		catch (NumberFormatException e) {
			// try the enum string
			httpStatus = HttpStatus.valueOf(statusString.toUpperCase(Locale.ROOT));
		}
		return httpStatus;
	}

	public static void addOriginalRequestUrl(ServerWebExchange exchange, URI url) {
		exchange.getAttributes().computeIfAbsent(GATEWAY_ORIGINAL_REQUEST_URL_ATTR, s -> new LinkedHashSet<>());
		LinkedHashSet<URI> uris = exchange.getRequiredAttribute(GATEWAY_ORIGINAL_REQUEST_URL_ATTR);
		uris.add(url);
	}

	public static AsyncPredicate<ServerWebExchange> toAsyncPredicate(Predicate<? super ServerWebExchange> predicate) {
		Assert.notNull(predicate, "predicate must not be null");
		return AsyncPredicate.from(predicate);
	}

	public static String expand(ServerWebExchange exchange, String template) {
		Assert.notNull(exchange, "exchange may not be null");
		Assert.notNull(template, "template may not be null");

		if (template.indexOf('{') == -1) { // short circuit
			return template;
		}

		Map<String, String> variables = getUriTemplateVariables(exchange);
		return UriComponentsBuilder.fromPath(template).build().expand(variables).getPath();
	}

	@SuppressWarnings("unchecked")
	public static void putUriTemplateVariables(ServerWebExchange exchange, Map<String, String> uriVariables) {
		if (exchange.getAttributes().containsKey(URI_TEMPLATE_VARIABLES_ATTRIBUTE)) {
			Map<String, Object> existingVariables = (Map<String, Object>) exchange.getAttributes()
				.get(URI_TEMPLATE_VARIABLES_ATTRIBUTE);
			HashMap<String, Object> newVariables = new HashMap<>();
			newVariables.putAll(existingVariables);
			newVariables.putAll(uriVariables);
			exchange.getAttributes().put(URI_TEMPLATE_VARIABLES_ATTRIBUTE, newVariables);
		}
		else {
			exchange.getAttributes().put(URI_TEMPLATE_VARIABLES_ATTRIBUTE, uriVariables);
		}
	}

	public static Map<String, String> getUriTemplateVariables(ServerWebExchange exchange) {
		return exchange.getAttributeOrDefault(URI_TEMPLATE_VARIABLES_ATTRIBUTE, new HashMap<>());
	}

	/**
	 * Caches the request body and the created {@link ServerHttpRequestDecorator} in
	 * ServerWebExchange attributes. Those attributes are
	 * {@link #CACHED_REQUEST_BODY_ATTR} and
	 * {@link #CACHED_SERVER_HTTP_REQUEST_DECORATOR_ATTR} respectively. This method is
	 * useful when the {@link ServerWebExchange} can not be modified, such as a
	 * {@link RoutePredicateFactory}.
	 * @param exchange the available ServerWebExchange.
	 * @param function a function that accepts the created ServerHttpRequestDecorator.
	 * @param <T> generic type for the return {@link Mono}.
	 * @return Mono of type T created by the function parameter.
	 */
	public static <T> Mono<T> cacheRequestBodyAndRequest(ServerWebExchange exchange,
			Function<ServerHttpRequest, Mono<T>> function) {
		return cacheRequestBody(exchange, true, function);
	}

	/**
	 * Caches the request body in a ServerWebExchange attributes. The attribute is
	 * {@link #CACHED_REQUEST_BODY_ATTR}. This method is useful when the
	 * {@link ServerWebExchange} can be mutated, such as a {@link GatewayFilterFactory}.
	 * @param exchange the available ServerWebExchange.
	 * @param function a function that accepts the created ServerHttpRequestDecorator.
	 * @param <T> generic type for the return {@link Mono}.
	 * @return Mono of type T created by the function parameter.
	 */
	public static <T> Mono<T> cacheRequestBody(ServerWebExchange exchange,
			Function<ServerHttpRequest, Mono<T>> function) {
		// 往下
		return cacheRequestBody(exchange, false, function);
	}

	/**
	 * Caches the request body in a ServerWebExchange attribute. The attribute is
	 * {@link #CACHED_REQUEST_BODY_ATTR}. If this method is called from a location that
	 * can not mutate the ServerWebExchange (such as a Predicate), setting
	 * cacheDecoratedRequest to true will put a {@link ServerHttpRequestDecorator} in an
	 * attribute {@link #CACHED_SERVER_HTTP_REQUEST_DECORATOR_ATTR} for adaptation later.
	 * @param exchange the available ServerWebExchange.
	 * @param cacheDecoratedRequest if true, the ServerHttpRequestDecorator will be
	 * cached.
	 * @param function a function that accepts a ServerHttpRequest. It can be the created
	 * ServerHttpRequestDecorator or the original if there is no body.
	 * @param <T> generic type for the return {@link Mono}.
	 * @return Mono of type T created by the function parameter.
	 */
	private static <T> Mono<T> cacheRequestBody(ServerWebExchange exchange, boolean cacheDecoratedRequest,
			Function<ServerHttpRequest, Mono<T>> function) {
		// don't cache if body is already cached
		Object cachedDataBuffer = exchange.getAttribute(CACHED_REQUEST_BODY_ATTR);
		if (cachedDataBuffer instanceof DataBuffer) {
			if (log.isTraceEnabled()) {
				log.trace("body already in exchange attribute, short circuiting");
			}
			return Mono.just(exchange.getRequest()).flatMap(function);
		}
		ServerHttpResponse response = exchange.getResponse();
		DataBufferFactory factory = response.bufferFactory();
		// Join all the DataBuffers so we have a single DataBuffer for the body
		return DataBufferUtils.join(exchange.getRequest().getBody())
				// 如果请求体是空的, 会创建一个空的 DataBuffer 对象
			.defaultIfEmpty(factory.wrap(EMPTY_BYTES))
				// decorate 包装了一下, 然后调用外面的function
			.map(dataBuffer -> decorate(exchange, dataBuffer, cacheDecoratedRequest))
				//如果是空, 这边就直接把 原始的请求返回
			.switchIfEmpty(Mono.just(exchange.getRequest()))
				// 调用外面的function
			.flatMap(function);
	}

	/**
	 * clear the request body in a ServerWebExchange attribute. The attribute is
	 * {@link #CACHED_REQUEST_BODY_ATTR}.
	 * @param exchange the available ServerWebExchange.
	 */
	public static void clearCachedRequestBody(ServerWebExchange exchange) {
		Object attribute = exchange.getAttributes().remove(CACHED_REQUEST_BODY_ATTR);

		// 如果是这个类型就会进行回收
		if (attribute != null && attribute instanceof PooledDataBuffer) {
			PooledDataBuffer dataBuffer = (PooledDataBuffer) attribute;
			if (dataBuffer.isAllocated()) {
				if (log.isTraceEnabled()) {
					log.trace("releasing cached body in exchange attribute");
				}
				// ensure proper release
				while (!dataBuffer.release()) {
					// release() counts down until zero, will never be infinite loop
				}
			}
		}
	}

	private static ServerHttpRequest decorate(ServerWebExchange exchange, DataBuffer dataBuffer,
			boolean cacheDecoratedRequest) {
		// 判断有内容
		if (dataBuffer.readableByteCount() > 0) {
			if (log.isTraceEnabled()) {
				log.trace("retaining body in exchange attribute");
			}

			Object cachedDataBuffer = exchange.getAttribute(CACHED_REQUEST_BODY_ATTR);
			// don't cache if body is already cached
			if (!(cachedDataBuffer instanceof DataBuffer)) {
				exchange.getAttributes().put(CACHED_REQUEST_BODY_ATTR, dataBuffer);
			}
		}

		// 生成新的 Request对象, 返回出去
		ServerHttpRequest decorator = new ServerHttpRequestDecorator(exchange.getRequest()) {
			@Override
			public Flux<DataBuffer> getBody() {
				return Mono.fromSupplier(() -> {
					if (exchange.getAttribute(CACHED_REQUEST_BODY_ATTR) == null) {
						// probably == downstream closed or no body
						return null;
					}
					if (dataBuffer instanceof NettyDataBuffer) {
						NettyDataBuffer pdb = (NettyDataBuffer) dataBuffer;
						// 复制了一下
						return pdb.factory().wrap(pdb.getNativeBuffer().retainedSlice());
					}
					else if (dataBuffer instanceof DefaultDataBuffer) {
						DefaultDataBuffer ddf = (DefaultDataBuffer) dataBuffer;
						// 复制了一下
						return ddf.factory().wrap(Unpooled.wrappedBuffer(ddf.getNativeBuffer()).nioBuffer());
					}
					else {
						throw new IllegalArgumentException(
								"Unable to handle DataBuffer of type " + dataBuffer.getClass());
					}
				}).flux();
			}
		};
		if (cacheDecoratedRequest) {
			exchange.getAttributes().put(CACHED_SERVER_HTTP_REQUEST_DECORATOR_ATTR, decorator);
		}
		return decorator;
	}

	/**
	 * One place to handle forwarding using DispatcherHandler. Allows for common code to
	 * be reused.
	 * @param handler The DispatcherHandler.
	 * @param exchange The ServerWebExchange.
	 * @return value from handler.
	 */
	public static Mono<Void> handle(DispatcherHandler handler, ServerWebExchange exchange) {
		// remove attributes that may disrupt the forwarded request
		exchange.getAttributes().remove(GATEWAY_PREDICATE_PATH_CONTAINER_ATTR);

		// CORS check is applied to the original request, but should not be applied to
		// internally forwarded requests.
		// See https://github.com/spring-cloud/spring-cloud-gateway/issues/3350.
		exchange = exchange.mutate().request(request -> request.headers(headers -> headers.setOrigin(null))).build();

		return handler.handle(exchange);
	}

}
