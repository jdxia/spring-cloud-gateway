package org.springframework.cloud.gateway.sample.filter;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 鉴权, 适用于 后台网关

@Slf4j
public class ValidateUrlFilter implements GlobalFilter {

	@Resource
	private WebClient webLBClient;

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		ServerHttpRequest request = exchange.getRequest();
		ServerHttpResponse response = exchange.getResponse();

		// 以下urls不需要校验url, 后台这些url放开
		List<String> urls = new ArrayList<>();
		urls.add("/validateUrl");
		urls.add("/validateToken");
		urls.add("/login");
		urls.add("/index");

		String requestUrl = request.getPath().value();

		if (!urls.contains(requestUrl)) {

			Integer uid = exchange.getRequiredAttribute("uid");

			Map<String, Object> body = new HashMap<>();
			body.put("uid", uid);
			body.put("url", request.getMethod() + "|" + requestUrl);

			return webLBClient.post()
					.uri("http://auth/validateUrl")
					.contentType(MediaType.APPLICATION_JSON)

					// 将 Map 对象转换为 JSON 格式作为 HTTP 请求体
					// body 包含 uid（用户ID）和 url（请求方法|请求路径）
					// 发送到认证服务进行 URL 权限校验
					.body(BodyInserters.fromValue(body))
					.retrieve()
					.bodyToMono(String.class)
					.then(Mono.defer(() -> {
						return chain.filter(exchange);
					}))
					.doOnError(e -> System.out.println("校验url失败: " + e.getMessage())) // 打印错误信息
					.onErrorResume(e -> {
						// 当出现错误时，返回“校验token失败”的响应，并且不执行后续的chain逻辑
						response.setStatusCode(HttpStatus.FORBIDDEN); // 设置合适的HTTP状态码
						response.getHeaders().add("Content-Type", "text/plain;charset=UTF-8");
						byte[] bytes = "校验url失败".getBytes(StandardCharsets.UTF_8);
						DataBuffer buffer = response.bufferFactory().wrap(bytes);
						return response.writeWith(Mono.just(buffer));
					});

		} else {
			return chain.filter(exchange);
		}
	}
}
