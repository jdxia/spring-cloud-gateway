package org.springframework.cloud.gateway.sample.utils;

public class AssertIdeaRunUtils {

    public static boolean runWithJarLauncher() {
        // 当前可能没有这个类, 不能用 JarLauncher.class.getName 方式获取, 用的地方可能有
        return isPresent("org.springframework.boot.loader.launch.JarLauncher");
    }

    public static boolean isPresent(String className) {
        try {
            Class.forName(className);
            return true;
        } catch (Exception ignore) {
            return false;
        }
    }

}
