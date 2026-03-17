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

package org.springframework.cloud.gateway.handler.predicate;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import jakarta.validation.ValidationException;
import jakarta.validation.constraints.NotNull;

import org.springframework.cloud.gateway.support.NameUtils;
import org.springframework.validation.annotation.Validated;

import static org.springframework.util.StringUtils.tokenizeToStringArray;

/**
 * @author Spencer Gibb
 */
@Validated
public class PredicateDefinition {
	/**
	 * predicates:
	 *   - After=2017-01-20T17:42:47.789-07:00[America/Denver]
	 *
	 * PredicateDefinition {
	 * 			name='After',
	 * 			args={_genkey_0=2017-01-20T17:42:47.789-07:00[America/Denver]}
	 * }
	 */

	/**
	 * 定义了 Predicate 的名称，它们要符固定的命名规范，为对应的工厂名称
	 *
	 * 断言的名称，与 {@link AbstractRoutePredicateFactory} 的子类名称前缀相同
	 */
	@NotNull
	private String name;

	/**
	 *  断言的参数  key:_genkey_0 value:/login
	 *
	 *  一个 Map 类型的参数，构造 Predicate 使用到的键值对参数
	 */
	private Map<String, String> args = new LinkedHashMap<>();

	public PredicateDefinition() {
	}

	/**
	 * 根据 text 创建 PredicateDefinition
	 *
	 * @param text 格式 ${name}=${args[0]},${args[1]}...${args[n]}
	 *             例如 Host=iocoder.cn
	 */
	public PredicateDefinition(String text) {
		int eqIdx = text.indexOf('=');
		if (eqIdx <= 0) {
			throw new ValidationException(
					"Unable to parse PredicateDefinition text '" + text + "'" + ", must be of the form name=value");
		}

		// name
		setName(text.substring(0, eqIdx));

		// args
		// 将配置的字符串参数中"="右边的字符串以","分割
		String[] args = tokenizeToStringArray(text.substring(eqIdx + 1), ",");

		// 遍历","分割后的结果，随机生成一个key(_genkey_+参数下标)，value为参数
		for (int i = 0; i < args.length; i++) {
			this.args.put(NameUtils.generateName(i), args[i]);
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Map<String, String> getArgs() {
		return args;
	}

	public void setArgs(Map<String, String> args) {
		this.args = args;
	}

	public void addArg(String key, String value) {
		this.args.put(key, value);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		PredicateDefinition that = (PredicateDefinition) o;
		return Objects.equals(name, that.name) && Objects.equals(args, that.args);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, args);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("PredicateDefinition{");
		sb.append("name='").append(name).append('\'');
		sb.append(", args=").append(args);
		sb.append('}');
		return sb.toString();
	}

}
