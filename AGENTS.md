# AGENTS.md

- 代码搜索用serena mcp
- 总是用中文回复我
- 一些api和文档可以用context7和mcp-deepwiki
- 帮我看下这个类的这个方法
1. 相关代码有没有扩展点
2. 实际工作中对这块会怎么用或者可以怎么扩展
3. 深入讲下这块源码和相关上下游的调用链
4. 讲下这块为啥要这么设计, 并且这块主要在处理什么事情
5. 学习源码,怎么实现的, 每行都要详细解释, 包括他调用的和调用他的地方

## 项目概述

这是 **Spring Cloud Gateway** 仓库，一个基于 Spring WebFlux 的 API 网关实现。版本为 **4.3.4-SNAPSHOT**，基于：
- Java 17
- Spring Framework 6
- Spring Boot 3
- Reactor Netty



## 模块架构

### 核心模块（重点关注）
- **spring-cloud-gateway-server-webflux** - WebFlux 网关核心实现（当前活跃）
- **spring-cloud-gateway-server-webmvc** - WebMvc 网关实现（Spring MVC 方式）
- **spring-cloud-starter-gateway-server-webflux** - WebFlux 入口 starter
- **spring-cloud-starter-gateway-server-webmvc** - WebMvc 入口 starter


### 示例与测试
- **spring-cloud-gateway-sample** - 示例网关应用（含 Nacos 集成、自定义 Filter/Predicate）
- spring-cloud-gateway-integration-tests - 集成测试（需要 Docker）
- user-demo - 用户服务演示

## 核心架构概念

### 三大核心组件
1. **Route（路由）** - 路由定义，包含 id、uri、predicates、filters
2. **Predicate（谓词）** - 路由匹配条件，继承 `AbstractRoutePredicateFactory`
3. **Filter（过滤器）** - 请求/响应处理，分为：
   - `GatewayFilter` - 路由级过滤器，继承 `AbstractGatewayFilterFactory`
   - `GlobalFilter` - 全局过滤器，直接实现 `GlobalFilter` 接口

### 关键类位置
- **路由匹配**：`org.springframework.cloud.gateway.handler.RoutePredicateHandlerMapping`
- **过滤器链执行**：`org.springframework.cloud.gateway.handler.FilteringWebHandler`
- **路由定义**：`org.springframework.cloud.gateway.route.RouteDefinition`
- **路由定位器**：`org.springframework.cloud.gateway.route.RouteDefinitionRouteLocator`

### 请求处理流程
```
HTTP Request → RoutePredicateHandlerMapping（匹配路由）
           → FilteringWebHandler（构建过滤器链）
           → GatewayFilterChain（执行过滤器）
           → Proxy（转发到后端服务）
           → Response（返回客户端）
```

## sample 模块特殊配置

### Nacos 集成
- 服务发现：`spring.cloud.nacos.discovery`
- 配置中心：`spring.cloud.nacos.config`
- 动态配置导入：`spring.config.import: optional:nacos:${spring.application.name}`

### 负载均衡健康检查
```yaml
spring.cloud.loadbalancer:
  configurations: health-check
  health-check:
    refetchInstances: true
    refetchInstancesInterval: 5s
    path:
      user-demo: /api/user-demo/actuator/health/readiness
```

### HTTP 客户端配置（连接池）
```yaml
spring.cloud.gateway.server.webflux.httpclient:
  pool:
    type: fixed
    max-connections: 200
    leasing-strategy: lifo  # 热连接优先
    acquire-timeout: 3000
```

### Actuator 网关管理端点
- `GET /actuator/gateway/routes` - 路由列表
- `GET /actuator/gateway/routes/{id}` - 指定路由详情
- `GET /actuator/gateway/globalfilters` - 全局过滤器列表
- `POST /actuator/gateway/refresh` - 刷新路由缓存

## 代码风格约定

- Java 17 语法
- 缩进：使用 Tab，4 个空格宽度（见 `.editorconfig`）
- 格式化：使用 Spring Java Format 插件（`spring-javaformat-maven-plugin`）
- Checkstyle：需通过 `maven-checkstyle-plugin` 验证
- 变更应保持向后兼容
- 避免过深嵌套和重复分支
- 复杂逻辑需注释说明"为什么"而非"做了什么"

## 自定义扩展开发

### 自定义 Predicate
```java
// 继承 AbstractRoutePredicateFactory<ConfigClass>
// 命名规范：XxxRoutePredicateFactory
// 配置使用：XxxPredicate=arg1,arg2
```

### 自定义 Filter
```java
// 路由级：继承 AbstractGatewayFilterFactory<ConfigClass>
// 全局级：实现 GlobalFilter 接口，标注 @Component
// 命名规范：XxxGatewayFilterFactory
// 配置使用：Xxx 或配置 name: Xxx
```

### 注意事项
1. 路由过滤器缓存默认关闭：`routeFilterCacheEnabled: false`（注意共享变量问题）
2. 请求相关数据存储在 `ServerWebExchange` 的 attributes 中
3. WebSocket 连接受 `max-life-time` 影响，会被强制关闭
4. 跨集群调用需配置 `spring.cloud.loadbalancer.nacos.enabled: true`
