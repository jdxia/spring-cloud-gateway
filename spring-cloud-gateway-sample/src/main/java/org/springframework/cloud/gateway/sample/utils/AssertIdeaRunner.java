package org.springframework.cloud.gateway.sample.utils;

import org.springframework.core.env.Environment;

public class AssertIdeaRunner {

    public static final String ALLOW_IDEA = "allow.idea";

    public static Boolean skipRunner(Environment environment) {
        // 默认认为 -Dallow.idea=false
        if (environment.getProperty(ALLOW_IDEA, Boolean.class, Boolean.FALSE)) { // true代表 允许
            return Boolean.FALSE;
        }

        // jar 启动是 false
        if (AssertIdeaRunUtils.runWithJarLauncher()) {
            return Boolean.FALSE;
        }

        return Boolean.TRUE;
    }

}
