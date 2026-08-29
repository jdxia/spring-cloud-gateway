package org.springframework.cloud.gateway.sample.filter;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

/**
 * 记录 Gateway 路由请求和响应的基础 HTTP 信息。
 *
 * <p>该过滤器只旁路复制有限长度且明确放行媒体类型的正文，不消费、替换或释放原始
 * DataBuffer，从而保留请求转发、流式写出、背压以及 Reactor Netty 服务端压缩语义。
 *
 * <p>仅建议用于本地学习、问题定位或受控采样。生产环境不能无条件记录完整正文，
 * 正文中可能包含密码、Token、身份证号、银行卡号等敏感信息。
 */
@Component
@Slf4j
public final class HttpTrafficLoggingGlobalFilter implements GlobalFilter, Ordered {

	/**
	 * 每个请求、响应最多保留前 8KB。
	 *
	 * <p>这只是日志采集上限，不是 HTTP 请求体大小限制。
	 */
	private static final int MAX_CAPTURE_BYTES = 8 * 1024;

	/**
	 * 只允许记录明确放行的 application 类型正文。
	 *
	 * <p>这里按 subtype 判断以兼容 charset 等参数，但不会把其他
	 * {@code application/*+json} 类型意外纳入日志。{@code application/javascript}
	 * 并不是 JSON 媒体类型，只因当前白名单明确要求而保留。
	 */
	private static final Set<String> LOGGABLE_APPLICATION_SUBTYPES = Set.of(
			"json",
			"problem+json",
			"javascript"
	);

	private final ObjectMapper objectMapper;

	public HttpTrafficLoggingGlobalFilter(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		/*
		 * DEBUG 关闭时完全不包装请求/响应，避免在正常流量下产生复制和日志开销。
		 */
		if (!log.isDebugEnabled()) {
			return chain.filter(exchange);
		}

		Instant startTime = Instant.now();
		long startNanos = System.nanoTime();
		ServerHttpRequest originalRequest = exchange.getRequest();
		ServerHttpResponse originalResponse = exchange.getResponse();

		BodyCapture requestBody = BodyCapture.forRequest(originalRequest.getHeaders());
		BodyCapture responseBody = BodyCapture.forResponse();

		ServerHttpRequest decoratedRequest = decorateRequest(originalRequest, requestBody);
		ServerHttpResponse decoratedResponse = decorateResponse(originalResponse, responseBody);

		ServerWebExchange decoratedExchange = exchange.mutate()
				.request(decoratedRequest)
				.response(decoratedResponse)
				.build();

		/*
		 * doFinally 能覆盖成功、异常和客户端取消。
		 *
		 * 此时响应状态、响应头以及已经经过装饰器的正文片段都已经可以读取。
		 * 如果请求在中途取消，日志会明确标记 CANCEL，并输出已观察到的部分数据。
		 */
		return chain.filter(decoratedExchange)
				.doFinally(signalType -> logExchangeSafely(
						decoratedExchange,
						originalRequest.getHeaders(),
						requestBody,
						responseBody,
						signalType,
						startTime,
						startNanos
				));
	}

	private void logExchangeSafely(
			ServerWebExchange exchange,
			Map<String, List<String>> requestHeaders,
			BodyCapture requestBody,
			BodyCapture responseBody,
			SignalType signalType,
			Instant startTime,
			long startNanos) {

		try {
			logExchange(exchange, requestHeaders, requestBody, responseBody, signalType, startTime, startNanos);
		}
		catch (RuntimeException ex) {
			/*
			 * 外部系统的脏 Header、Body 或日志序列化都不能制造新的网关故障。
			 */
			log.warn("[gateway-access] Failed to build access log, requestId={}", exchange.getRequest().getId(), ex);
		}
	}

	private ServerHttpRequest decorateRequest(ServerHttpRequest request, BodyCapture capture) {
		return new ServerHttpRequestDecorator(request) {
			@Override
			public Flux<DataBuffer> getBody() {
				/*
				 * 这里不订阅请求体，只在真正的消费者读取请求体时旁路复制。
				 * 原 DataBuffer 会继续交给下游，读指针和引用计数都不变。
				 */
				return super.getBody().doOnNext(capture::append);
			}
		};
	}

	private ServerHttpResponse decorateResponse(ServerHttpResponse response, BodyCapture capture) {
		return new ServerHttpResponseDecorator(response) {
			@Override
			public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
				/*
				 * 在交给底层写出前记录 Content-Encoding。
				 *
				 * 如果此时已经是 gzip，通常表示后端返回的是压缩数据；
				 * 如果 gzip 是后面的 Reactor Netty 服务端压缩添加的，此处还看不到，
				 * 因而采集到的是压缩前正文。
				 */
				capture.captureMetadata(getHeaders());

				return super.writeWith(
						Flux.from(body).doOnNext(capture::append)
				);
			}

			@Override
			public Mono<Void> writeAndFlushWith(
					Publisher<? extends Publisher<? extends DataBuffer>> body) {

				capture.captureMetadata(getHeaders());

				/*
				 * 不能把嵌套 Publisher 直接 flatten 成普通 Flux<DataBuffer>。
				 * 外层的每个 Publisher 代表一次 flush 边界，SSE、流式响应会依赖它。
				 */
				return super.writeAndFlushWith(
						Flux.from(body)
								.map(part -> Flux.from(part).doOnNext(capture::append))
				);
			}
		};
	}

	private void logExchange(
			ServerWebExchange exchange,
			Map<String, List<String>> requestHeaders,
			BodyCapture requestBody,
			BodyCapture responseBody,
			SignalType signalType,
			Instant startTime,
			long startNanos) {

		ServerHttpRequest request = exchange.getRequest();
		ServerHttpResponse response = exchange.getResponse();
		HttpStatusCode statusCode = response.getStatusCode();
		Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
		Instant endTime = Instant.now();

		Map<String, Object> accessLog = new LinkedHashMap<>();
		accessLog.put("requestId", request.getId());
		accessLog.put("routeId", route == null ? null : route.getId());
		accessLog.put("scheme", request.getURI().getScheme());
		accessLog.put("requestMethod", request.getMethod().name());
		accessLog.put("requestUrl", request.getURI().getRawPath());
		accessLog.put("queryParams", request.getQueryParams());
		accessLog.put("requestHeaders", requestHeaders);
		accessLog.put("requestBody", requestBody.render(this.objectMapper));
		accessLog.put("remoteIp", getRemoteAddress(request));
		accessLog.put("responseBody", responseBody.render(this.objectMapper));
		accessLog.put("responseHeaders", response.getHeaders());
		accessLog.put("httpStatus", statusCode == null ? null : statusCode.value());
		accessLog.put("signal", signalType.name());
		accessLog.put("startTime", startTime.toString());
		accessLog.put("endTime", endTime.toString());
		accessLog.put("durationMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));

		writeAccessLog(accessLog);
	}

	private void writeAccessLog(Map<String, Object> accessLog) {
		try {
			log.debug("[gateway-access] {}", this.objectMapper.writerWithDefaultPrettyPrinter()
					.writeValueAsString(accessLog));
		}
		catch (JsonProcessingException ex) {
			/*
			 * 日志是旁路能力，序列化失败时只能降级，不能反向破坏网关请求。
			 */
			log.warn("[gateway-access] Failed to serialize access log, requestId={}",
					accessLog.get("requestId"), ex);
		}
	}

	private static String getRemoteAddress(ServerHttpRequest request) {
		return request.getRemoteAddress() == null ? null : request.getRemoteAddress().getHostString();
	}

	@Override
	public int getOrder() {
		/*
		 * AdaptCachedBodyGlobalFilter 的顺序是 HIGHEST_PRECEDENCE + 1000。
		 *
		 * 放在它后面，可以兼容 ReadBody Predicate 已缓存并重放请求体的场景；
		 * 同时仍早于普通路由过滤器、响应缓存和 NettyWriteResponseFilter，
		 * 因而能包住后续请求/响应链路。
		 */
		return Ordered.HIGHEST_PRECEDENCE + 2000;
	}

	private static final class BodyCapture {

		private final int maxCaptureBytes;

		private final ByteArrayOutputStream captured;

		private long totalBytes;

		private MediaType contentType;

		private String contentEncoding;

		private boolean metadataCaptured;

		private BodyCapture(int maxCaptureBytes) {
			this.maxCaptureBytes = maxCaptureBytes;
			this.captured = new ByteArrayOutputStream(maxCaptureBytes);
		}

		static BodyCapture forRequest(HttpHeaders headers) {
			BodyCapture capture = new BodyCapture(MAX_CAPTURE_BYTES);
			capture.captureMetadata(headers);
			return capture;
		}

		static BodyCapture forResponse() {
			return new BodyCapture(MAX_CAPTURE_BYTES);
		}

		void captureMetadata(HttpHeaders headers) {
			if (this.metadataCaptured) {
				return;
			}

			this.metadataCaptured = true;
			this.contentEncoding = headers.getFirst(HttpHeaders.CONTENT_ENCODING);

			try {
				this.contentType = headers.getContentType();
			}
			catch (IllegalArgumentException ignored) {
				/*
				 * 外部系统可能返回非法 Content-Type。
				 * 日志过滤器不能因为一个脏响应头破坏正常代理链路。
				 */
				this.contentType = null;
			}
		}

		void append(DataBuffer dataBuffer) {
			int readableBytes = dataBuffer.readableByteCount();
			this.totalBytes += readableBytes;

			if (!isLoggableBody() || this.captured.size() >= this.maxCaptureBytes) {
				return;
			}

			int copyLength = Math.min(
					readableBytes,
					this.maxCaptureBytes - this.captured.size()
			);

			byte[] bytes = new byte[copyLength];
			ByteBuffer target = ByteBuffer.wrap(bytes);

			/*
			 * 从当前 readPosition 复制数据，但不推进原 DataBuffer 的读指针。
			 * 不调用 read(...)，也不 retain/release。
			 */
			dataBuffer.toByteBuffer(
					dataBuffer.readPosition(),
					target,
					0,
					copyLength
			);

			this.captured.writeBytes(bytes);
		}

		Object render(ObjectMapper objectMapper) {
			if (this.totalBytes == 0) {
				return "<empty>";
			}

			if (hasNonIdentityEncoding()) {
				return "<encoded content-encoding="
						+ this.contentEncoding
						+ ", bytes="
						+ this.totalBytes
						+ ">";
			}

			if (!isLoggableContentType()) {
				return "<not logged content-type="
						+ this.contentType
						+ ", bytes="
						+ this.totalBytes
						+ ">";
			}

			String value = this.captured.toString(StandardCharsets.UTF_8);
			String safeValue = escapeLineBreaks(value);
			if (this.totalBytes > this.captured.size()) {
				return safeValue
						+ "...<truncated captured="
						+ this.captured.size()
						+ ", total="
						+ this.totalBytes
						+ ">";
			}

			if (!isJsonContentType()) {
				return safeValue;
			}

			try {
				return objectMapper.readTree(value);
			}
			catch (JsonProcessingException ignored) {
				return safeValue;
			}
		}

		private boolean isLoggableBody() {
			return !hasNonIdentityEncoding() && isLoggableContentType();
		}

		private boolean hasNonIdentityEncoding() {
			return this.contentEncoding != null
					&& !"identity".equalsIgnoreCase(this.contentEncoding);
		}

		private boolean isLoggableContentType() {
			if (this.contentType == null) {
				return false;
			}

			String subtype = this.contentType.getSubtype()
					.toLowerCase(Locale.ROOT);

			return "application".equalsIgnoreCase(this.contentType.getType())
					&& LOGGABLE_APPLICATION_SUBTYPES.contains(subtype);
		}

		private boolean isJsonContentType() {
			String subtype = this.contentType.getSubtype().toLowerCase(Locale.ROOT);
			return "json".equals(subtype) || "problem+json".equals(subtype);
		}

		private static String escapeLineBreaks(String value) {
			return value.replace("\r", "\\r")
					.replace("\n", "\\n");
		}
	}
}
