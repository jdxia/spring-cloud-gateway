package org.springframework.cloud.gateway.sample.config;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.gateway.config.GatewayProperties;
import org.springframework.cloud.gateway.handler.predicate.PredicateDefinition;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * 网关启动后主动发请求预热，触发 LoadBalancer 缓存填充。
 *
 * 原理：Gateway 的 LoadBalancer 是 Lazy 的，只有第一个请求进来才会从 Nacos 拉取服务实例列表。
 * 启动后主动发请求，让 LoadBalancer 提前缓存实例 IP，后续请求就不会有冷启动延迟。
 *
 * 自动从 GatewayProperties 中提取所有 lb:// 路由，按服务去重后，拼接健康检查路径进行预热。
 * 新增 lb:// 路由后无需修改此类，只要路由配了 Path predicate 就会自动预热。
 *
 * 注意事项：
 * 1. 预热失败不影响网关启动，只会打 warn 日志
 * 2. 下游服务此时不一定启动完成，404/503 都正常 —— 目的只是触发 LoadBalancer 缓存
 * 3. 如果某个路由的 Path predicate 格式特殊（如不带前缀的 /** ），需要检查日志确认拼出来的路径是否合理
 * 4. 只包含 yaml/properties 里配置的路由，代码方式（RouteLocatorBuilder）定义的路由不在范围内
 *
 * 配置开关：gateway.warmup.enabled=true（默认开启），生产环境可按需关闭
 */
@Component
@ConditionalOnProperty(name = "gateway.warmup.enabled", havingValue = "true", matchIfMissing = true)
public class GatewayWarmUpRunner implements ApplicationListener<ApplicationStartedEvent>, ApplicationContextAware, Ordered {

	private static final Logger log = LoggerFactory.getLogger(GatewayWarmUpRunner.class);

	private ConfigurableApplicationContext applicationContext;

	/**
	 * 统一的下游服务健康检查路径。
	 * 预热时会拼接为：/{路由Path前缀}/{HEALTH_CHECK_PATH}
	 * 例如路由 Path=/user/** → 预热请求 GET /user/actuator/health/readiness
	 */
	private static final String HEALTH_CHECK_PATH = "actuator/health/readiness";

	private final WebClient webClient;
	private final GatewayProperties gatewayProperties;

	public GatewayWarmUpRunner(@Value("${server.port:8899}") int port,
			GatewayProperties gatewayProperties) {
		this.webClient = WebClient.builder()
				.baseUrl("http://127.0.0.1:" + port)
				.build();
		this.gatewayProperties = gatewayProperties;
	}

	@Override
	public void onApplicationEvent(ApplicationStartedEvent event) {
		doHandler();
	}

	private void doHandler() {
		List<WarmUpTarget> targets = resolveWarmUpTargets();

		if (CollectionUtils.isEmpty(targets)) {
			log.info("[Warmup] 没有发现 lb:// 路由，跳过预热");
			return;
		}

		log.info("[Warmup] 发现 {} 个 lb:// 服务需要预热: {}",
				targets.size(), targets.stream().map(t -> t.serviceId).toList());

		Flux.fromIterable(targets)
				.flatMap(this::warmUp)
				.collectList()
				.subscribe(
						results -> log.info("[Warmup] 预热完成，共处理 {} 个服务", results.size()),
						error -> log.error("[Warmup] 预热过程发生未预期的错误", error)
				);
	}

	/**
	 * 从 GatewayProperties 中提取所有 lb:// 路由，按 serviceId 去重，
	 * 解析 Path predicate 前缀，拼接健康检查路径。
	 *
	 * 去重逻辑：同一个 serviceId（如 lb://user）只预热一次，因为 LoadBalancer 缓存是按 serviceId 维度的，
	 * 多个路由指向同一个服务时，预热一次就够了。
	 */
	private List<WarmUpTarget> resolveWarmUpTargets() {
		List<WarmUpTarget> targets = new ArrayList<>();
		Set<String> seenServiceIds = new HashSet<>();

		for (RouteDefinition route : gatewayProperties.getRoutes()) {
			URI uri = route.getUri();
			// 只处理 lb:// 协议的路由（需要 LoadBalancer 的）
			if (uri == null || !"lb".equalsIgnoreCase(uri.getScheme())) {
				continue;
			}

			// 按 serviceId 去重：lb://user 和 lb://user 只预热一次
			String serviceId = uri.getHost();
			if (serviceId == null || !seenServiceIds.add(serviceId)) {
				continue;
			}

			// 从 predicates 中找到 Path predicate，提取前缀
			String pathPrefix = extractPathPrefix(route);
			if (pathPrefix == null) {
				log.warn("[Warmup] 路由 [{}] (lb://{}) 没有 Path predicate，跳过", route.getId(), serviceId);
				continue;
			}

			String warmUpPath = pathPrefix + "/" + HEALTH_CHECK_PATH;
			targets.add(new WarmUpTarget(serviceId, route.getId(), warmUpPath));
			log.debug("[Warmup] 路由 [{}] lb://{} → 预热路径: {}", route.getId(), serviceId, warmUpPath);
		}

		return targets;
	}

	/**
	 * 从路由的 Path predicate 中提取路径前缀。
	 *
	 * Path=/user/**       → /user
	 * Path=/api/order/**  → /api/order
	 * Path=/echo          → /echo
	 *
	 * 原理：Path predicate 使用 shortcut 配置时，args 存储为 {_genkey_0: "/user/**", _genkey_1: "/user2/**", ...}
	 * 取第一个值，去掉尾部的通配符部分。
	 */
	private String extractPathPrefix(RouteDefinition route) {
		for (PredicateDefinition predicate : route.getPredicates()) {
			if (!"Path".equals(predicate.getName())) {
				continue;
			}

			Map<String, String> predicateArgs = predicate.getArgs();
			String pathPattern = predicateArgs.values().stream()
					.findFirst()
					.orElse(null);

			if (pathPattern == null || pathPattern.isBlank()) {
				continue;
			}

			// 去掉尾部通配符：/user/** → /user, /api/order/** → /api/order
			String prefix = pathPattern;
			int wildcardIdx = prefix.indexOf('*');
			if (wildcardIdx > 0) {
				prefix = prefix.substring(0, wildcardIdx);
			}
			// 也处理路径变量：/api/{version}/user 中的 {version} 无法解析，跳过这种路由
			if (prefix.contains("{")) {
				log.warn("[Warmup] 路由 [{}] 的 Path 包含路径变量 {}，无法自动预热，跳过", route.getId(), pathPattern);
				return null;
			}
			// 去掉末尾的斜杠：/user/ → /user
			if (prefix.endsWith("/")) {
				prefix = prefix.substring(0, prefix.length() - 1);
			}

			return prefix;
		}
		return null;
	}

	/**
	 * 对单个服务发预热请求。
	 *
	 * 使用 retrieve() + onStatus() 忽略所有 HTTP 错误状态码。
	 * 预热场景下，任何 HTTP 响应（包括 503）都说明 LoadBalancer 已触发实例拉取，属于预热成功。
	 * 只有连接失败、超时等网络层错误才算真正的失败。
	 */
	private Mono<String> warmUp(WarmUpTarget target) {
		return webClient.get()
				.uri(target.path)
				.retrieve()
				// 忽略所有 HTTP 错误状态码，预热只关心"有没有响应"，不关心状态码
				.onStatus(status -> true, response -> Mono.empty())
				.toBodilessEntity()
				.doOnNext(entity ->
					log.info("[Warmup] lb://{} ({}) -> status: {}", target.serviceId, target.path, entity.getStatusCode())
				)
				.map(entity -> target.serviceId)
				.timeout(Duration.ofSeconds(3))
				.onErrorResume(e -> {
					// 网络层错误（连接拒绝、超时等），不影响启动
					log.warn("[Warmup] lb://{} ({}) -> 网络错误: {}", target.serviceId, target.path, e.getMessage());
					return Mono.just(target.serviceId);
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
		// 优先级低一点
		return LOWEST_PRECEDENCE - 10;
	}

}
