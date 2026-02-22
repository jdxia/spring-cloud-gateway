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
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


// 认证

@Slf4j
public class ValidateTokenFilter implements GlobalFilter {

	@Resource
	private WebClient webLBClient;


	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		ServerHttpRequest request = exchange.getRequest();
		ServerHttpResponse response = exchange.getResponse();

		// url，需不需要验证token, 一般是不需要token的一些场景 还有 一些没有token的header
		List<String> urls = new ArrayList<>();
		urls.add("/validateToken");
		urls.add("/login");

		String path = exchange.getRequest().getPath().value();

		// localhost:28080/user/addUser
		if (!urls.contains(path)) {
			// 验证token

			// 调用auth微服务
			String token = exchange.getRequest().getHeaders().getFirst("token");

			return webLBClient.get()
					.uri("lb://auth/validateToken")
					.header("token", token)
//					.accept(MediaType.APPLICATION_JSON)

					// retrieve() 方法用于执行 HTTP 请求并获取响应
					// 它是 WebClient 的响应处理入口，后续可以通过 bodyToMono/bodyToFlux 等方法来提取响应体
					.retrieve()

					// 获取的用户的uid
					.bodyToMono(Integer.class)
					.flatMap(uid -> {
						// 验证token如果是正常的情况

						// mutate() 获取一个 Builder
						ServerHttpRequest newRequest = request.mutate().headers((headers) -> {
							headers.put("uid", List.of(String.valueOf(uid)));
						}).build();

						ServerWebExchange newExchange = exchange.mutate().request(newRequest).build();

						newExchange.getAttributes().put("uid", uid);

						return chain.filter(newExchange);
					})

					// 副作用操作（不改变流）, 只是"旁观者"，不会消费或处理错误
					.doOnError(e -> System.out.println("校验token失败: " + e.getMessage())) // 打印错误信息

					//  真正处理错误
					.onErrorResume(e -> {
						// 当出现错误时，返回“校验token失败”的响应，并且不执行后续的chain逻辑
						response.setStatusCode(HttpStatus.UNAUTHORIZED); // 设置合适的HTTP状态码
						response.getHeaders().add("Content-Type", "text/plain;charset=UTF-8");
						byte[] bytes = "校验token失败".getBytes(StandardCharsets.UTF_8);
						DataBuffer buffer = response.bufferFactory().wrap(bytes);
						return response.writeWith(Mono.just(buffer));
					});
		} else {
			return chain.filter(exchange);
		}
	}
}

