package org.springframework.cloud.gateway.sample.config;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;


import io.netty.channel.ChannelOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.cloud.gateway.config.GatewayProperties;
import org.springframework.cloud.gateway.handler.predicate.PredicateDefinition;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;


@Component
@ConditionalOnProperty(name = "gateway.warmup.enabled", havingValue = "true", matchIfMissing = true)
public class GatewayWarmUpRunner implements ApplicationListener<ApplicationStartedEvent>, ApplicationContextAware, Ordered {

	private static final Logger log = LoggerFactory.getLogger(GatewayWarmUpRunner.class);

	private ConfigurableApplicationContext applicationContext;

	private final WebClient warmUpWebClient;

	private final GatewayProperties gatewayProperties;

	private final ObjectProvider<HttpClient> httpClientProvider;

	public GatewayWarmUpRunner(@Value("${server.port:8899}") int port, GatewayProperties gatewayProperties,
							   ObjectProvider<HttpClient> httpClientProvider) {

		HttpClient httpClient = HttpClient.create()
				.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)  // 连接超时 2 秒
				.responseTimeout(Duration.ofSeconds(3));               // 响应超时 3 秒

		this.warmUpWebClient = WebClient.builder()
				.baseUrl("http://127.0.0.1:" + port)
				.clientConnector(new ReactorClientHttpConnector(httpClient))
				.build();
		this.gatewayProperties = gatewayProperties;
		this.httpClientProvider = httpClientProvider;
	}

	@Override
	public void onApplicationEvent(ApplicationStartedEvent event) {
		// 预热线程池
		warmUpExecutors();

		// 预热 Netty HttpClient（安全获取，Bean 不存在时跳过）
		warmUpHttpClient();

		warmUpLoadBalancerCache();
	}

	/**
	 * 预热 Netty HttpClient 连接池和 EventLoop。
	 * <p>
	 * 使用 ObjectProvider 安全获取，避免容器中没有 HttpClient Bean 时启动失败。
	 * 例如升级 Spring Cloud Gateway 版本后 @ConditionalOnMissingBean 条件变化，
	 * HttpClient Bean 可能不存在。
	 */
	private void warmUpHttpClient() {
		HttpClient client = httpClientProvider.getIfAvailable();
		if (client == null) {
			log.warn("[Warmup] 容器中未找到 HttpClient Bean，跳过 HttpClient 预热");
			return;
		}
		try {
			client.warmup().block();
			log.info("[Warmup] Netty HttpClient 预热完成");
		} catch (Exception ex) {
			// 预热失败不影响启动
			log.warn("[Warmup] Netty HttpClient 预热异常，已跳过: {}", ex.getMessage());
		}
	}

	/**
	 * 预热 Spring 容器中所有线程池的核心线程。
	 * <p>
	 * 覆盖范围：
	 * 1. ThreadPoolTaskExecutor（Spring 的通用异步线程池，如 @Async 默认线程池）
	 * 2. ThreadPoolTaskScheduler（Spring 的调度线程池，如 @Scheduled 使用的）
	 * 3. 直接注册为 Bean 的 ExecutorService（用户自定义的原生线程池）
	 * <p>
	 * 注意：Reactor Netty 内部的 EventLoop 线程不是 Spring Bean，无法通过此方式预热，
	 * 但预热 HTTP 请求会间接触发 Netty 线程初始化。
	 */
	private void warmUpExecutors() {
		// 用 IdentityHashMap 做去重，避免同一个底层 ThreadPoolExecutor 被预热多次
		// （比如 ThreadPoolTaskExecutor 内部包装了一个 ThreadPoolExecutor，两者是同一个实例）
		Set<ThreadPoolExecutor> prestarted = Collections.newSetFromMap(new IdentityHashMap<>());

		// 1. Spring 的 ThreadPoolTaskExecutor（最常见，@Async、自定义异步线程池等）
		Map<String, ThreadPoolTaskExecutor> taskExecutors = applicationContext.getBeansOfType(ThreadPoolTaskExecutor.class);
		for (Map.Entry<String, ThreadPoolTaskExecutor> entry : taskExecutors.entrySet()) {
			ThreadPoolExecutor executor = entry.getValue().getThreadPoolExecutor();
			if (prestarted.add(executor)) {
				executor.prestartAllCoreThreads();
				log.info("[Warmup] ThreadPoolTaskExecutor [{}] 预启动 ", entry.getKey());
			}
		}

		// 2. Spring 的 ThreadPoolTaskScheduler（@Scheduled、定时任务等）
		Map<String, ThreadPoolTaskScheduler> taskSchedulers = applicationContext.getBeansOfType(ThreadPoolTaskScheduler.class);
		for (Map.Entry<String, ThreadPoolTaskScheduler> entry : taskSchedulers.entrySet()) {
			ThreadPoolExecutor executor = entry.getValue().getScheduledThreadPoolExecutor();
			if (prestarted.add(executor)) {
				executor.prestartAllCoreThreads();
				log.info("[Warmup] ThreadPoolTaskScheduler [{}] 预启动", entry.getKey());
			}
		}

		// 3. 直接注册的 ExecutorService Bean（用户可能直接 @Bean 返回 ThreadPoolExecutor）
		Map<String, ExecutorService> executorServices = applicationContext.getBeansOfType(ExecutorService.class);
		for (Map.Entry<String, ExecutorService> entry : executorServices.entrySet()) {
			if (entry.getValue() instanceof ThreadPoolExecutor executor && prestarted.add(executor)) {
				int count = executor.prestartAllCoreThreads();
				log.info("[Warmup] ExecutorService [{}] 预启动", entry.getKey());
			}
		}

		log.info("[Warmup] 线程池预热完毕, 共 {} 个线程池", prestarted.size());
	}

	/**
	 * 预热 LoadBalancer 缓存：对每个 lb:// 服务发一次 HTTP 请求，触发实例列表拉取。
	 */
	private void warmUpLoadBalancerCache() {
		List<WarmUpTarget> targets = resolveWarmUpTargets();
		if (targets.isEmpty()) {
			log.warn("[Warmup] 没有发现 lb:// 路由，跳过 LoadBalancer 预热");
			return;
		}

		log.info("[Warmup] 发现 {} 个 lb:// 服务需要预热: {}",
				targets.size(), targets.stream().map(WarmUpTarget::serviceId).toList());

		Flux.fromIterable(targets)
				.flatMap(target ->
						Flux.range(0, 16).flatMap(i -> warmUp(target)))
				.then()
				.timeout(Duration.ofSeconds(30))
				.onErrorResume(e -> {
					log.warn("[Warmup] LoadBalancer 预热超时或异常，已跳过", e);
					return Mono.empty();
				})
				.block();

		log.info("[Warmup] LoadBalancer 预热流程结束");
	}

	/**
	 * 从 GatewayProperties 中提取所有 lb:// 路由，按 serviceId 去重，
	 * 解析 Path predicate 前缀作为预热路径。
	 */
	private List<WarmUpTarget> resolveWarmUpTargets() {
		Set<String> seen = new HashSet<>();
		List<WarmUpTarget> targets = new ArrayList<>();

		for (RouteDefinition route : gatewayProperties.getRoutes()) {
			URI uri = route.getUri();
			if (uri == null || !"lb".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null) {
				continue;
			}
			String serviceId = uri.getHost();
			String prefix = extractPathPrefix(route);
			if (prefix == null) {
				log.warn("[Warmup] 路由 [{}] (lb://{}) 没有可用的 Path predicate，跳过", route.getId(), serviceId);
				continue;
			}
			// Path 解析成功后再按 serviceId 去重
			if (!seen.add(serviceId)) {
				continue;
			}
			targets.add(new WarmUpTarget(serviceId, route.getId(), prefix + "/actuator/health/readiness"));
		}
		return targets;
	}

	/**
	 * 从路由的 Path predicate 中提取路径前缀。
	 * Path=/user/**       → /user
	 * Path=/api/order/**  → /api/order
	 */
	private String extractPathPrefix(RouteDefinition route) {
		for (PredicateDefinition predicate : route.getPredicates()) {
			if (!"Path".equals(predicate.getName())) {
				continue;
			}
			String pattern = predicate.getArgs().values().stream().findFirst().orElse(null);
			if (pattern == null || pattern.isBlank()) {
				continue;
			}
			// 路径变量无法解析，跳过
			if (pattern.contains("{")) {
				log.warn("[Warmup] 路由 [{}] 的 Path 包含路径变量 {}，无法自动预热，跳过", route.getId(), pattern);
				continue;
			}
			// /user/** → /user
			String prefix = pattern.contains("*") ? pattern.substring(0, pattern.indexOf('*')) : pattern;
			if (prefix.endsWith("/")) {
				prefix = prefix.substring(0, prefix.length() - 1);
			}
			return prefix;
		}
		return null;
	}

	/**
	 * 对单个服务发预热请求，任何响应（含非2xx）都算触发了 LoadBalancer 缓存。
	 * 网络层错误不影响网关启动。
	 */
	private Mono<Void> warmUp(WarmUpTarget target) {
		return warmUpWebClient.get()
				.uri(target.path())
				.exchangeToMono(response -> {
					int statusCode = response.statusCode().value();
					if (statusCode == 200) {
						log.info("[Warmup] 预热成功 lb://{} ({}) -> {}", target.serviceId(), target.path(), statusCode);
					} else {
						log.error("[Warmup] 预热失败, 状态码非200 lb://{} ({}) -> {}", target.serviceId(), target.path(), statusCode);
					}
					return response.releaseBody();
				})
				.timeout(Duration.ofSeconds(3))
				.onErrorResume(e -> {
					log.error("[Warmup] 预热失败 lb://{} ({})", target.serviceId(), target.path(), e);
					return Mono.empty();
				});
	}

	/**
	 * 预热目标，包含服务信息和预热路径
	 */
	private record WarmUpTarget(String serviceId, String routeId, String path) {
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	@Override
	public int getOrder() {
		return LOWEST_PRECEDENCE - 10;
	}

}
