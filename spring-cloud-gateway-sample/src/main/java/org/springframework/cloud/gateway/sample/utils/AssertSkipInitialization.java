package org.springframework.cloud.gateway.sample.utils;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.Objects;

public class AssertSkipInitialization implements Condition {

    private static final String BOOTSTRAP_PROPERTY_SOURCE = "bootstrap";

    private static final String REFRESH_PROPERTY_SOURCE = "refreshArgs";

    private static final String SPRING_NONE_SOURCE = "spring.main.sources";

    private static final String APPLICATION_NONE_TYPE = "spring.main.web-application-type";

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        ConfigurableEnvironment environment = (ConfigurableEnvironment) context.getEnvironment();
        //取反, 适配 条件注解
        return !shouldSkipInitialization(environment);
    }

    public static Boolean shouldSkipInitialization(ConfigurableEnvironment environment) {

        MutablePropertySources propertySources = environment.getPropertySources();
        if (Objects.isNull(propertySources)) {
            return Boolean.FALSE;
        }

        // 启动的时候, 在spring cloud 的config容器里面
        if (propertySources.contains(BOOTSTRAP_PROPERTY_SOURCE)) {
            return Boolean.TRUE;
        }

        // 在refresh的时候, 自动创建的
        if (propertySources.contains(REFRESH_PROPERTY_SOURCE) &&
                "".equals(environment.getProperty(SPRING_NONE_SOURCE)) &&
                "NONE".equals(environment.getProperty(APPLICATION_NONE_TYPE))) {
            return Boolean.TRUE;
        }


        return Boolean.FALSE;
    }
}
