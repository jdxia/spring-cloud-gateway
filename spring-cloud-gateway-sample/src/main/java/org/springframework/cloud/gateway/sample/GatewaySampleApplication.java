package org.springframework.cloud.gateway.sample;

import java.util.Locale;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.client.loadbalancer.reactive.ReactorLoadBalancerExchangeFilterFunction;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RouterFunctions.route;


@SpringBootConfiguration
@EnableAutoConfiguration
@ComponentScan("org.springframework.cloud.gateway.sample")
@Slf4j
public class GatewaySampleApplication {
	/**
	 * 源码先看这几个文件
	 * 1. 自动装配的 spring-cloud-gateway-server/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
	 * 2. 其他的一些 spring-cloud-gateway-server/src/main/resources/META-INF/spring.factories
	 * <p>
	 * <p>
	 * 内置谓词都是在这个文件夹里 spring-cloud-gateway-server/src/main/java/org/springframework/cloud/gateway/handler/predicate
	 *
	 */

	public static void main(String[] args) {
		System.setProperty("nacos.logging.default.config.enabled", "false");

		/**
		 * 检测Reactor/Netty IO 线程上的阻塞操作
		 * 要添加 <artifactId>blockhound</artifactId>
		 *
		 * JDK 17+ 需要额外 JVM 参数，
		 *
		 * 因为模块系统限制了反射访问： --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED
		 * 下面是全一点的
		 * --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.math=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.security=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/jdk.internal.access=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/jdk.internal.perf=ALL-UNNAMED --add-exports java.base/jdk.internal.perf=ALL-UNNAMED --add-opens java.management/sun.management.counter.perf=ALL-UNNAMED --add-opens java.management/sun.management.counter=ALL-UNNAMED
		 *
		 *  还需要加 -XX:+AllowRedefinitionToAddDeleteMethods
		 */

		/**
		 * 检测IO线程上的阻塞调用，生产环境务必移除
		 * 不用 BlockHound.install() 是因为它会走 ServiceLoader 自动发现,
		 * Nacos shaded jar 里注册了一个找不到的 BlockHound SPI 类会导致启动失败
		 */
		BlockHound.builder()
				// 白名单
				.allowBlockingCallsInside("ch.qos.logback.classic.Logger", "callAppenders")
				// 只打日志不抛异常，记录完整堆栈
				.blockingMethodCallback(m -> {
					Error error = new Error(m.toString());
					log.error("[BlockHound] 检测到IO线程阻塞调用", error);
				})
				.install();

		SpringApplication.run(GatewaySampleApplication.class, args);
	}


	/**
	 * 支持负载均衡
	 */
	@Bean
	public WebClient webLBClient(ReactorLoadBalancerExchangeFilterFunction lb) {
		return WebClient.builder()
				.filter(lb)
				.build();
	}

	@Bean
	public RouterFunction<ServerResponse> user() {
		return route()
				.GET("/index", request -> {
					return ServerResponse.status(HttpStatus.OK).body(BodyInserters.fromValue("hello gateway!"));
				})
				.build();
	}

	@Bean
	public RouteLocator customRouteLocator(RouteLocatorBuilder builder) { // ① RouteLocatorBuilder bean 在 spring-cloud-starter-gateway 模块自动装配类中已经声明，可直接使用。RouteLocator 封装了对 Route 获取的定义，可简单理解成工厂模式
		return builder.routes() // ② RouteLocatorBuilder 可以构建多个路由信息

				/**
				 * ③ 指定了 Predicates，这里包含两个：
				 * 请求头Host需要匹配**.abc.org，通过 HostRoutePredicateFactory 产生
				 * 请求路径需要匹配/image/png，通过 PathRoutePredicateFactory 产生
				 */
				.route(r -> r.host("**.abc.org").and().path("/image/png")
						.filters(f ->
								// ④ 指定了一个 Filter，下游服务响应后添加响应头X-TestHeader:foobar，通过AddResponseHeaderGatewayFilterFactory 产生
								f.addResponseHeader("X-TestHeader", "foobar"))
						// ⑤ 指定路由转发的目的地 uri
						.uri("http://httpbin.org:80")
				)
				.build();
	}


/**
 public static final String HELLO_FROM_FAKE_ACTUATOR_METRICS_GATEWAY_REQUESTS = "hello from fake /actuator/metrics/spring.cloud.gateway.requests";

 @Value("${test.uri:http://httpbin.org:80}") String uri;

 @Bean public RouteLocator customRouteLocator(RouteLocatorBuilder builder) {
 //@formatter:off
		// String uri = "http://httpbin.org:80";
		// String uri = "http://localhost:9080";
		return builder.routes()
				.route(r -> r.host("**.abc.org").and().path("/anything/png")
					.filters(f ->
							f.prefixPath("/httpbin")
									.addResponseHeader("X-TestHeader", "foobar"))
					.uri(uri)
				)
				.route("read_body_pred", r -> r.host("*.readbody.org")
						.and().readBody(String.class,
										s -> s.trim().equalsIgnoreCase("hi"))
					.filters(f -> f.prefixPath("/httpbin")
							.addResponseHeader("X-TestHeader", "read_body_pred")
					).uri(uri)
				)
				.route("rewrite_request_obj", r -> r.host("*.rewriterequestobj.org")
					.filters(f -> f.prefixPath("/httpbin")
							.addResponseHeader("X-TestHeader", "rewrite_request")
							.modifyRequestBody(String.class, Hello.class, MediaType.APPLICATION_JSON_VALUE,
									(exchange, s) -> {
										return Mono.just(new Hello(s.toUpperCase(Locale.ROOT)));
									})
					).uri(uri)
				)
				.route("rewrite_request_upper", r -> r.host("*.rewriterequestupper.org")
					.filters(f -> f.prefixPath("/httpbin")
							.addResponseHeader("X-TestHeader", "rewrite_request_upper")
							.modifyRequestBody(String.class, String.class,
									(exchange, s) -> {
										return Mono.just(s.toUpperCase(Locale.ROOT) + s.toUpperCase(Locale.ROOT));
									})
					).uri(uri)
				)
				.route("rewrite_response_upper", r -> r.host("*.rewriteresponseupper.org")
					.filters(f -> f.prefixPath("/httpbin")
							.addResponseHeader("X-TestHeader", "rewrite_response_upper")
							.modifyResponseBody(String.class, String.class,
									(exchange, s) -> {
										return Mono.just(s.toUpperCase(Locale.ROOT));
									})
					).uri(uri)
				)
				.route("rewrite_empty_response", r -> r.host("*.rewriteemptyresponse.org")
					.filters(f -> f.prefixPath("/httpbin")
							.addResponseHeader("X-TestHeader", "rewrite_empty_response")
							.modifyResponseBody(String.class, String.class,
									(exchange, s) -> {
										if (s == null) {
											return Mono.just("emptybody");
										}
										return Mono.just(s.toUpperCase(Locale.ROOT));
									})

					).uri(uri)
				)
				.route("rewrite_response_fail_supplier", r -> r.host("*.rewriteresponsewithfailsupplier.org")
					.filters(f -> f.prefixPath("/httpbin")
							.addResponseHeader("X-TestHeader", "rewrite_response_fail_supplier")
							.modifyResponseBody(String.class, String.class,
									(exchange, s) -> {
										if (s == null) {
											return Mono.error(new IllegalArgumentException("this should not happen"));
										}
										return Mono.just(s.toUpperCase(Locale.ROOT));
									})
					).uri(uri)
				)
				.route("rewrite_response_obj", r -> r.host("*.rewriteresponseobj.org")
					.filters(f -> f.prefixPath("/httpbin")
							.addResponseHeader("X-TestHeader", "rewrite_response_obj")
							.modifyResponseBody(Map.class, String.class, MediaType.TEXT_PLAIN_VALUE,
									(exchange, map) -> {
										Object data = map.get("data");
										return Mono.just(data.toString());
									})
							.setResponseHeader("Content-Type", MediaType.TEXT_PLAIN_VALUE)
					).uri(uri)
				)
				.route(r -> r.path("/image/webp")
					.filters(f ->
							f.prefixPath("/httpbin")
									.addResponseHeader("X-AnotherHeader", "baz"))
					.uri(uri)
				)
				.build();
		//@formatter:on
 }

 @Bean public RouterFunction<ServerResponse> testFunRouterFunction() {
 RouterFunction<ServerResponse> route = RouterFunctions.route(RequestPredicates.path("/testfun"),
 request -> ServerResponse.ok().body(BodyInserters.fromValue("hello")));
 return route;
 }

 @Bean public RouterFunction<ServerResponse> testWhenMetricPathIsNotMeet() {
 RouterFunction<ServerResponse> route = RouterFunctions.route(
 RequestPredicates.path("/actuator/metrics/spring.cloud.gateway.requests"),
 request -> ServerResponse.ok()
 .body(BodyInserters.fromValue(HELLO_FROM_FAKE_ACTUATOR_METRICS_GATEWAY_REQUESTS)));
 return route;
 }

 static class Hello {

 String message;

 Hello() {
 }

 Hello(String message) {
 this.message = message;
 }

 public String getMessage() {
 return message;
 }

 public void setMessage(String message) {
 this.message = message;
 }

 }
 */


}
