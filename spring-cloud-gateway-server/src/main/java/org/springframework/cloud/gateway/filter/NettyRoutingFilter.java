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

import java.net.URI;
import java.time.Duration;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.gateway.config.HttpClientProperties;
import org.springframework.cloud.gateway.filter.headers.HttpHeadersFilter;
import org.springframework.cloud.gateway.filter.headers.HttpHeadersFilter.Type;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.TimeoutException;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBuffer;
import org.springframework.core.io.buffer.NettyDataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.AbstractServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;

import static org.springframework.cloud.gateway.filter.headers.HttpHeadersFilter.filterRequest;
import static org.springframework.cloud.gateway.support.RouteMetadataUtils.CONNECT_TIMEOUT_ATTR;
import static org.springframework.cloud.gateway.support.RouteMetadataUtils.RESPONSE_TIMEOUT_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.CLIENT_RESPONSE_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.CLIENT_RESPONSE_CONN_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.CLIENT_RESPONSE_HEADER_NAMES;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.ORIGINAL_RESPONSE_CONTENT_TYPE_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.PRESERVE_HOST_HEADER_ATTRIBUTE;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.isAlreadyRouted;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.setAlreadyRouted;

/**
 * @author Spencer Gibb
 * @author Biju Kunjummen
 *
 * NettyRoutingFilter处理schema为http/https的请求，使用基于Netty HttpClient请求后端的服务, 发起请求到后端服务, 请求转发 + 响应头处理
 *
 * {@link NettyWriteResponseFilter} 用来处理NettyRoutingFilter请求后端获得的响应，将响应写回给客户端。
 *
 * 同时SCG还定义了WebClientHttpRoutingFilter，于NettyRoutingFilter类似，区别在于没有使用Netty去做请求转发的代理。
 * NettyRoutingFilter中会将请求的响应放入上下文中，供 {@link NettyWriteResponseFilter} 使用
 *
 * client =1=> {@link NettyRoutingFilter}  =2=> 后端服务
 * 后端服务 =3=>  {@link NettyRoutingFilter}
 *  {@link NettyRoutingFilter} =4=> {@link NettyWriteResponseFilter}
 *  {@link NettyWriteResponseFilter} =5=> client
 */
public class NettyRoutingFilter implements GlobalFilter, Ordered {

	/**
	 * The order of the NettyRoutingFilter. See {@link Ordered#LOWEST_PRECEDENCE}.
	 */
	public static final int ORDER = Ordered.LOWEST_PRECEDENCE;

	private static final Log log = LogFactory.getLog(NettyRoutingFilter.class);

	private final HttpClient httpClient;

	private final ObjectProvider<List<HttpHeadersFilter>> headersFiltersProvider;

	private final HttpClientProperties properties;

	// do not use this headersFilters directly, use getHeadersFilters() instead.
	private volatile List<HttpHeadersFilter> headersFilters;

	public NettyRoutingFilter(HttpClient httpClient, ObjectProvider<List<HttpHeadersFilter>> headersFiltersProvider,
			HttpClientProperties properties) {
		this.httpClient = httpClient;
		this.headersFiltersProvider = headersFiltersProvider;
		this.properties = properties;
	}

	public List<HttpHeadersFilter> getHeadersFilters() {
		if (headersFilters == null) {
			headersFilters = headersFiltersProvider.getIfAvailable();
		}
		return headersFilters;
	}

	@Override
	public int getOrder() {
		return ORDER;
	}

	/**
	 * 在负载均衡之后, 选出一个之后
	 */
	@Override
	@SuppressWarnings("Duplicates")
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		// 最终转发的目标 URL
		/**
		 * 从上下文中获取在{@link RouteToRequestUrlFilter} 中放入的请求URL
		 */
		URI requestUrl = exchange.getRequiredAttribute(GATEWAY_REQUEST_URL_ATTR);

		//获取请求的协议
		String scheme = requestUrl.getScheme();

		// 看是什么协议
		//如果该请求已经被处理过，或者请求协议不是http/https，则不处理
		if (isAlreadyRouted(exchange) || (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme))) {
			return chain.filter(exchange);
		}

		// 设置请求已被路由
		setAlreadyRouted(exchange);

		//获取请求
		ServerHttpRequest request = exchange.getRequest();

		//获取请求方式
		final HttpMethod method = HttpMethod.valueOf(request.getMethod().name());

		//获取请求的URI
		final String url = requestUrl.toASCIIString();

		// http 请求头的过滤器
		//执行请求头Filter，如ForwardedHeadersFilter、RemoveHopByHopHeadersFilter、XForwardedHeadersFilter
		HttpHeaders filtered = filterRequest(getHeadersFilters(), exchange);

		final DefaultHttpHeaders httpHeaders = new DefaultHttpHeaders();
		//基于filter过后的请求头创建Http请求头
		filtered.forEach(httpHeaders::set);

		boolean preserveHost = exchange.getAttributeOrDefault(PRESERVE_HOST_HEADER_ATTRIBUTE, false);
		//获取路由
		Route route = exchange.getAttribute(GATEWAY_ROUTE_ATTR);

		/**
		 * 先构造http client
		 * 创建HttpClient
		 */
		Flux<HttpClientResponse> responseFlux = getHttpClientMono(route, exchange)
			.flatMapMany(httpClient -> httpClient.headers(headers -> {
				//添加请求头
				headers.add(httpHeaders);

				// 移除 host
				// Will either be set below, or later by Netty
				headers.remove(HttpHeaders.HOST);
				if (preserveHost) {
					String host = request.getHeaders().getFirst(HttpHeaders.HOST);
					headers.add(HttpHeaders.HOST, host);
				}
			}).request(method).uri(url).send((req, nettyOutbound) -> {
				if (log.isTraceEnabled()) {
					nettyOutbound.withConnection(connection -> log.trace("outbound route: "
							+ connection.channel().id().asShortText() + ", inbound: " + exchange.getLogPrefix()));
				}

				// 发送数据
				return nettyOutbound.send(request.getBody().map(this::getByteBuf));
			}).responseConnection((res, connection) -> {

				/**
				 * 响应的存起来
				 * 将响应和连接存入 exchange，供 {@link NettyWriteResponseFilter} 使用
				 */
				// Defer committing the response until all route filters have run
				// Put client response as ServerWebExchange attribute and write
				// response later NettyWriteResponseFilter
				//将调用真实服务返回的Response放入上下文，但NettyWriteResponseFilter中也没有用
				exchange.getAttributes().put(CLIENT_RESPONSE_ATTR, res);

				//将Netty Channle放入上下文供NettyWriteResponseFilter使用
				exchange.getAttributes().put(CLIENT_RESPONSE_CONN_ATTR, connection);

				// 拿到响应对象
				ServerHttpResponse response = exchange.getResponse();
				// put headers and status so filters can modify the response
				HttpHeaders headers = new HttpHeaders();

				res.responseHeaders().forEach(entry -> headers.add(entry.getKey(), entry.getValue()));

				String contentTypeValue = headers.getFirst(HttpHeaders.CONTENT_TYPE);
				if (StringUtils.hasLength(contentTypeValue)) {
					exchange.getAttributes().put(ORIGINAL_RESPONSE_CONTENT_TYPE_ATTR, contentTypeValue);
				}

				// 设置响应对象的状态
				setResponseStatus(res, response);

				// make sure headers filters run after setting status so it is
				// available in response
				HttpHeaders filteredResponseHeaders = HttpHeadersFilter.filter(getHeadersFilters(), headers, exchange,
						Type.RESPONSE);

				if (!filteredResponseHeaders.containsKey(HttpHeaders.TRANSFER_ENCODING)
						&& filteredResponseHeaders.containsKey(HttpHeaders.CONTENT_LENGTH)) {
					// It is not valid to have both the transfer-encoding header and
					// the content-length header.
					// Remove the transfer-encoding header in the response if the
					// content-length header is present.
					response.getHeaders().remove(HttpHeaders.TRANSFER_ENCODING);
				}

				exchange.getAttributes().put(CLIENT_RESPONSE_HEADER_NAMES, filteredResponseHeaders.keySet());

				// 设置 响应头
				response.getHeaders().addAll(filteredResponseHeaders);

				return Mono.just(res);
			}));

		//获取响应超时时间
		Duration responseTimeout = getResponseTimeout(route);
		if (responseTimeout != null) {
			//设置获取响应超时时间
			responseFlux = responseFlux
				.timeout(responseTimeout,
						Mono.defer(() -> Mono
							.error(new TimeoutException("Response took longer than timeout: " + responseTimeout))))
				.onErrorMap(TimeoutException.class,
						th -> new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT, th.getMessage(), th));
		}

		return responseFlux.then(chain.filter(exchange));
	}

	protected ByteBuf getByteBuf(DataBuffer dataBuffer) {
		if (dataBuffer instanceof NettyDataBuffer) {
			NettyDataBuffer buffer = (NettyDataBuffer) dataBuffer;
			return buffer.getNativeBuffer();
		}
		// MockServerHttpResponse creates these
		else if (dataBuffer instanceof DefaultDataBuffer) {
			DefaultDataBuffer buffer = (DefaultDataBuffer) dataBuffer;
			return Unpooled.wrappedBuffer(buffer.getNativeBuffer());
		}
		throw new IllegalArgumentException("Unable to handle DataBuffer of type " + dataBuffer.getClass());
	}

	private void setResponseStatus(HttpClientResponse clientResponse, ServerHttpResponse response) {
		HttpStatus status = HttpStatus.resolve(clientResponse.status().code());
		if (status != null) {
			response.setStatusCode(status);
		}
		else {
			while (response instanceof ServerHttpResponseDecorator) {
				response = ((ServerHttpResponseDecorator) response).getDelegate();
			}
			if (response instanceof AbstractServerHttpResponse) {
				((AbstractServerHttpResponse) response).setRawStatusCode(clientResponse.status().code());
			}
			else {
				// TODO: log warning here, not throw error?
				throw new IllegalStateException("Unable to set status code " + clientResponse.status().code()
						+ " on response of type " + response.getClass().getName());
			}
		}
	}

	/**
	 * Creates a new HttpClient with per route timeout configuration. Sub-classes that
	 * override, should call super.httpClient() if they want to honor the per route
	 * timeout configuration.
	 * @param route the current route.
	 * @param exchange the current ServerWebExchange.
	 * @return the configured HttpClient.
	 */
	protected Mono<HttpClient> getHttpClientMono(Route route, ServerWebExchange exchange) {
		return Mono.just(getHttpClient(route, exchange));
	}

	/**
	 * Creates a new HttpClient with per route timeout configuration. Sub-classes that
	 * override, should call super.getHttpClient() if they want to honor the per route
	 * timeout configuration.
	 * @param route the current route.
	 * @param exchange the current ServerWebExchange.
	 * @return the configured HttpClient.
	 */
	protected HttpClient getHttpClient(Route route, ServerWebExchange exchange) {
		Object connectTimeoutAttr = route.getMetadata().get(CONNECT_TIMEOUT_ATTR) != null
				? route.getMetadata().get(CONNECT_TIMEOUT_ATTR) : properties.getConnectTimeout();
		if (connectTimeoutAttr != null) {
			Integer connectTimeout = getInteger(connectTimeoutAttr);
			return this.httpClient.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
		}
		return httpClient;
	}

	static Integer getInteger(Object connectTimeoutAttr) {
		Integer connectTimeout;
		if (connectTimeoutAttr instanceof Integer) {
			connectTimeout = (Integer) connectTimeoutAttr;
		}
		else {
			connectTimeout = Integer.parseInt(connectTimeoutAttr.toString());
		}
		return connectTimeout;
	}

	private Duration getResponseTimeout(Route route) {
		try {
			if (route.getMetadata().containsKey(RESPONSE_TIMEOUT_ATTR)) {
				Long routeResponseTimeout = getLong(route.getMetadata().get(RESPONSE_TIMEOUT_ATTR));
				if (routeResponseTimeout != null && routeResponseTimeout >= 0) {
					return Duration.ofMillis(routeResponseTimeout);
				}
				else {
					return null;
				}
			}
		}
		catch (NumberFormatException e) {
			// ignore number format and use global default
		}
		return properties.getResponseTimeout();
	}

	static Long getLong(Object responseTimeoutAttr) {
		Long responseTimeout = null;
		if (responseTimeoutAttr instanceof Number) {
			responseTimeout = ((Number) responseTimeoutAttr).longValue();
		}
		else if (responseTimeoutAttr != null) {
			responseTimeout = Long.parseLong(responseTimeoutAttr.toString());
		}
		return responseTimeout;
	}

}
