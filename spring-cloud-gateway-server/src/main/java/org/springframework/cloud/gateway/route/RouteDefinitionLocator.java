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

package org.springframework.cloud.gateway.route;

import org.springframework.cloud.gateway.config.PropertiesRouteDefinitionLocator;
import org.springframework.cloud.gateway.discovery.DiscoveryClientRouteDefinitionLocator;
import reactor.core.publisher.Flux;

/**
 * @author Spencer Gibb
 *
 * 负责读取路由配置
 * 1. {@link PropertiesRouteDefinitionLocator} 从配置文件( 例如，YML / Properties 等 ) 读取
 * 2. {@link RouteDefinitionRepository}  从存储器( 例如，内存 / Redis / MySQL 等 )读取
 * 3. {@link DiscoveryClientRouteDefinitionLocator} 从注册中心( 例如，Eureka / Consul / Zookeeper / Etcd 等 )读取
 * 4. {@link CompositeRouteDefinitionLocator} 组合多种 RouteDefinitionLocator 的实现，为 RouteDefinitionRouteLocator 提供统一入口
 * 5. {@link org.springframework.cloud.gateway.route.CachingRouteDefinitionLocator} 也是 RouteDefinitionLocator 的实现类，已经被 CachingRouteLocator 取代
 *
 */
public interface RouteDefinitionLocator {

	// 获取所有的路由信息
	Flux<RouteDefinition> getRouteDefinitions();

}
