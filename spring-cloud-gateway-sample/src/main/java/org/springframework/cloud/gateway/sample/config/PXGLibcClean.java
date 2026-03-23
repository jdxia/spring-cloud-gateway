package org.springframework.cloud.gateway.sample.config;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.sample.utils.AssertIdeaRunner;
import org.springframework.cloud.gateway.sample.utils.AssertSkipInitialization;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Locale;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class PXGLibcClean implements ApplicationContextInitializer<ConfigurableApplicationContext>, Ordered {

    private static final String COMMAND_ARG = "System.trim_native_heap";

    // 预编译正则：匹配 RSS before 和 after 值
    private static final Pattern RSS_PATTERN = Pattern.compile("RSS\\s+before:\\s*(\\d+)k,\\s*after:\\s*(\\d+)k");

    private static final Logger log = LoggerFactory.getLogger(PXGLibcClean.class);
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);

    // 配置常量
    public static final String GLIBC_ENABLED = "px.glibc.enabled";
    /**
     * 如果是定时的, 那就不支持凌晨  执行
     * 模拟 jdk高版本这个参数
     * -XX:TrimNativeHeapInterval=millis
     * Interval, in ms, at which the JVM will trim the native heap. Lower values will reclaim memory more eagerly at the cost of higher overhead. A value of 0 (default) disables native heap trimming. Native heap trimming is performed in a dedicated thread.
     *
     * This option is only supported on Linux with GNU C Library (glibc).
     */
    public static final String TRIM_HOURS = "px.glibc.trim.hours";

    // 线程池（用于调度和执行）
    private ScheduledThreadPoolExecutor scheduler;
    private int trimHours;

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE - 3;
    }

    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        ConfigurableEnvironment environment = applicationContext.getEnvironment();

        // 不要被bootstrap容器拉起来
        if (AssertSkipInitialization.shouldSkipInitialization(environment)) {
            return;
        }

        // 本地启动跳过
        if (Boolean.TRUE.equals(AssertIdeaRunner.skipRunner(environment))) {
            log.info("[PXGLibcClean] 本地IDEA启动，跳过");
            return;
        }

        // 配置开关检查
        if (!environment.getProperty(GLIBC_ENABLED, Boolean.class, true)) {
            log.info("[PXGLibcClean] 功能已关闭");
            return;
        }

        // 环境检查
        if (!isJava17()) {
            log.warn("[PXGLibcClean] JDK < 17，不支持 {}", COMMAND_ARG);
            return;
        }

        if (!isLinux() || !isGlibc()) {
            log.info("[PXGLibcClean] 非 Linux 系统 或者 没有用glibc，跳过");
            return;
        }

        // 防止重复初始化
        if (!INITIALIZED.compareAndSet(false, true)) {
            log.info("[PXGLibcClean] 已初始化，跳过");
            return;
        }

        log.info("===> [PXGLibcClean] 开始初始化 <===");

        startScheduler(environment);
    }

    /**
     * 启动调度器
     */
    private void startScheduler(ConfigurableEnvironment environment) {
        try {
            trimHours = environment.getProperty(TRIM_HOURS, Integer.class, 0);

            // 创建单线程调度器
            CustomizableThreadFactory threadFactory = new CustomizableThreadFactory("px-glibc-trim-");
            threadFactory.setDaemon(true);
            threadFactory.setThreadPriority(Thread.MIN_PRIORITY); // 设置低优先级
            scheduler = new ScheduledThreadPoolExecutor(1, threadFactory, new ThreadPoolExecutor.DiscardPolicy());
            scheduler.setMaximumPoolSize(1);
            scheduler.setRemoveOnCancelPolicy(true);

            // 注册关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (scheduler != null) {
                    scheduler.shutdownNow();
                    log.info("[PXGLibcClean] 调度器已停止");
                }
            }, "px-glibc-trim-shutdown"));


            scheduleNextTask(calculateNextDelay(), TimeUnit.MILLISECONDS);

            if (trimHours > 0) {
                log.info("===> [PXGLibcClean] 初始化成功，每 {} 小时执行一次 <===", trimHours);
            } else {
                log.info("===> [PXGLibcClean] 初始化成功，每天凌晨 3-5 点随机时间执行 <===");
            }

        } catch (Throwable t) {
            log.error("[PXGLibcClean] 初始化失败", t);
        }
    }

    /**
     * 安排下次任务
     */
    private void scheduleNextTask(long delay, TimeUnit unit) {
        scheduler.schedule(() -> {
            try {
                executeTrim();
            } finally {
                // 执行完后计算下次执行时间
                long nextDelay = calculateNextDelay();
                scheduleNextTask(nextDelay, TimeUnit.MILLISECONDS);
            }
        }, delay, unit);
    }

    /**
     * 计算下次执行延迟时间（毫秒）
     */
    private long calculateNextDelay() {
        if (trimHours > 0) {
            // 固定间隔模式
            long delayMillis = TimeUnit.HOURS.toMillis(trimHours);
            log.info("[PXGLibcClean] 下次执行：{} 小时后", trimHours);
            return delayMillis;
        }

        // 每日模式：凌晨 + 多重随机，避免多实例同时执行
        ZoneId zone = ZoneId.systemDefault();

        // 多层随机化策略：
        // 1. 基础小时随机化：3-5点之间
        int randomHour = ThreadLocalRandom.current().nextInt(3, 6);  // 3, 4, 5点
        // 2. 分钟随机化：1-59分钟
        int randomMinutes = ThreadLocalRandom.current().nextInt(1, 59);
        // 3. 秒级随机化：1-59秒，进一步分散执行时间
        int randomSeconds = ThreadLocalRandom.current().nextInt(1, 59);

        LocalDateTime target = LocalDate.now(zone)
                .plusDays(1)
                .atTime(randomHour, randomMinutes, randomSeconds);

        ZonedDateTime targetTime = target.atZone(zone);

        long delayMillis = Duration.between(Instant.now(), targetTime.toInstant()).toMillis();
        log.info("[PXGLibcClean] 下次执行：{}", targetTime);
        return delayMillis;
    }

    /**
     * 执行内存清理
     */
    private void executeTrim() {
        // 防止并发执行
        if (!RUNNING.compareAndSet(false, true)) {
            log.warn("[PXGLibcClean] 上次任务仍在执行，跳过");
            return;
        }

        try {
            // 执行清理 用 jcmd
            invokeTrimViaExternalJcmd();

        } catch (Throwable t) {
            log.error("[PXGLibcClean] 执行失败", t);
        } finally {
            RUNNING.set(false);
        }
    }

    /**
     * 通过 jcmd 命令调用 System.trim_native_heap
     */
    private void invokeTrimViaExternalJcmd() {
        Process process = null;
        try {
            log.info("[PXGLibcClean] jcmd <pid> {} 开始执行", COMMAND_ARG);
            // /usr/local/jdk/
            String javaHome = System.getProperty("java.home", "/usr/local/jdk/");
            String jcmdPath = new File(javaHome, "bin/jcmd").getAbsolutePath();
            long pid = ProcessHandle.current().pid();

            // 执行 jcmd 命令
            process = new ProcessBuilder(jcmdPath, String.valueOf(pid), COMMAND_ARG)
                    .redirectErrorStream(true)
                    .start();

            // 读取输出
            StringBuilder output = new StringBuilder();

            try (InputStream inputStream = process.getInputStream();
                 InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                 BufferedReader bufferedReader = new BufferedReader(reader)) {

                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }

            // 假设 process 是一个 Process 实例
            // 指定超时时间为10秒
            if (process.waitFor(10, TimeUnit.SECONDS)) {
                // 正常退出，可以安全地获取退出码
                int exitCode = process.exitValue();
                if (exitCode == 0) {
                    log.info("[PXGLibcClean] jcmd 调用成功: {}", output.toString().trim());
                } else {
                    log.error("[PXGLibcClean] jcmd 调用失败: exitCode={}", exitCode);
                }

            } else {
                log.error("[PXGLibcClean] jcmd {} 调用超时", COMMAND_ARG);
                process.destroyForcibly();
            }

        } catch (Throwable t) {
            log.error("[PXGLibcClean] jcmd {} 调用异常:", COMMAND_ARG, t);
        } finally {
            // 确保进程被正确关闭
            if (process != null) {
                try {
                    if (process.isAlive()) {
                        process.destroyForcibly();
                    }
                } catch (Throwable t) {
                    log.error("[PXGLibcClean] 关闭 jcmd {} 进程时发生异常:", COMMAND_ARG, t);
                }
            }
        }
    }

    /**
     * 检测是否为 glibc 环境
     */
    private boolean isGlibc() {
        Process process = null;
        log.info("[PXGLibcClean] 开始检测 glibc");
        try {
            process = new ProcessBuilder("ldd", "--version")
                    .redirectErrorStream(true)
                    .start();

            try (InputStream inputStream = process.getInputStream();
                 InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                 BufferedReader bufferedReader = new BufferedReader(reader)) {

                String firstLine = bufferedReader.readLine();
                if (firstLine != null) {
                    String lower = firstLine.toLowerCase(Locale.ROOT);
                    if (lower.contains("glibc") || lower.contains("gnu libc")) {
                        log.info("[PXGLibcClean] 检测到 glibc");
                        return true;
                    }
                    if (lower.contains("musl")) {
                        log.info("[PXGLibcClean] 检测到 musl libc，不支持");
                    }
                }
            }
            process.waitFor(5, TimeUnit.SECONDS);
        } catch (Throwable t) {
            log.warn("[PXGLibcClean] glibc 检测失败: ", t);
        } finally {
            // 确保进程被正确关闭
            if (process != null) {
                try {
                    if (process.isAlive()) {
                        process.destroyForcibly();
                    }
                } catch (Throwable t) {
                    log.error("[PXGLibcClean] glibc 检测进程时发生异常:", t);
                }
            }
        }
        return false;
    }

    /**
     * 检查是否为 JDK 17+
     */
    private boolean isJava17() {
        try {
            return Runtime.version().feature() >= 17;
        } catch (Throwable t) {
            return false;
        }
    }

    /**
     * 检查是否为 Linux 系统
     */
    private boolean isLinux() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux");
    }
}
