package com.study.config;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.time.LocalDateTime;

/**
 * Actuator 端点访问日志过滤器
 */
@Configuration
public class ActuatorLogFilter {

	private static final Logger log = LoggerFactory.getLogger(ActuatorLogFilter.class);

	@Bean
	public FilterRegistrationBean<Filter> actuatorLoggingFilter() {
		FilterRegistrationBean<Filter> registration = new FilterRegistrationBean<>();
		registration.setFilter(new Filter() {
			@Override
			public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
					throws IOException, ServletException {
				
				HttpServletRequest httpRequest = (HttpServletRequest) request;
				HttpServletResponse httpResponse = (HttpServletResponse) response;
				
				String requestURI = httpRequest.getRequestURI();
				String method = httpRequest.getMethod();
				String remoteAddr = getClientIpAddress(httpRequest);
				
				log.info("【Actuator访问】时间: {}, 方法: {}, 路径: {}, 客户端IP: {}",
						LocalDateTime.now(), method, requestURI, remoteAddr);
				
				long startTime = System.currentTimeMillis();
				
				try {
					chain.doFilter(request, response);
				} finally {
					long duration = System.currentTimeMillis() - startTime;
					int status = httpResponse.getStatus();
					log.info("【Actuator响应】路径: {}, 状态码: {}, 耗时: {}ms", requestURI, status, duration);
				}
			}
			
			private String getClientIpAddress(HttpServletRequest request) {
				String ip = request.getHeader("X-Forwarded-For");
				if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
					ip = request.getHeader("X-Real-IP");
				}
				if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
					ip = request.getRemoteAddr();
				}
				if (ip != null && ip.contains(",")) {
					ip = ip.split(",")[0].trim();
				}
				return ip;
			}
		});
		
		// 只拦截 /actuator/** 路径
		registration.addUrlPatterns("/actuator/*");
		registration.setName("actuatorLoggingFilter");
		registration.setOrder(1);
		
		return registration;
	}
}
