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

    /**
     * 剩余内存告警阈值，默认 500MB（单位 KB）
     * 当容器 memory limit - RSS before < 此值时，打印 error 日志
     */
    private static final long REMAINING_MEMORY_THRESHOLD_KB = 500L * 1024; // 500MB

    // cgroup v2 memory limit 文件路径
    private static final String CGROUP_V2_MEMORY_MAX = "/sys/fs/cgroup/memory.max";
    // cgroup v1 memory limit 文件路径
    private static final String CGROUP_V1_MEMORY_LIMIT = "/sys/fs/cgroup/memory/memory.limit_in_bytes";

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
                    // 提取 RSS after 的值并判断是否 >= 74 开头
                    extractAndLogRssAfter(output.toString());
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
	 * 7:
	 * Attempting trim...
	 * Done.
	 * Virtual size before: 41419608k, after: 41419204k, (-404k)
	 * RSS before: 7336112k, after: 7010796k, (-325316k)
	 * Swap before: 0k, after: 0k, (0k)
	 *
     * 提取 RSS before 的值，结合容器 memory limit 判断剩余内存是否不足
     * 如果 limit - before < 500MB 则输出 error 日志
     * 日志格式示例: RSS before: 7336112k, after: 7010796k, (-325316k)
     */
    private void extractAndLogRssAfter(String output) {
        if (output == null || output.isEmpty()) {
            return;
        }

		try {
			Matcher matcher = RSS_PATTERN.matcher(output);
			if (matcher.find()) {
				String beforeValue = matcher.group(1);
				String afterValue = matcher.group(2);
				log.info("[PXGLibcClean] RSS before: {}k", beforeValue);
				log.info("[PXGLibcClean] RSS after: {}k", afterValue);

				long beforeKB = Long.parseLong(beforeValue);

				// 获取容器 memory limit，计算剩余可用内存
				long limitKB = getContainerMemoryLimitKB();
				if (limitKB > 0) {
					long remainingKB = limitKB - beforeKB;
					long remainingMB = remainingKB / 1024;
					log.info("[PXGLibcClean] 容器 memory limit: {}MB, RSS before: {}MB, 剩余: {}MB",
							limitKB / 1024, beforeKB / 1024, remainingMB);

					if (remainingKB < REMAINING_MEMORY_THRESHOLD_KB) {
						log.error("[PXGLibcClean] 剩余内存不足! limit: {}MB, RSS: {}MB, 剩余仅: {}MB (阈值: {}MB), "
										+ "建议把一些RocketMQ线程调低或者其他内存调低给堆外腾出空间",
								limitKB / 1024, beforeKB / 1024, remainingMB,
								REMAINING_MEMORY_THRESHOLD_KB / 1024);
					}
				} else {
					// 无法获取 limit 时，保留原来的兜底逻辑：前两位 >= 76 才告警
					if (beforeValue.length() >= 2 && Integer.parseInt(beforeValue.substring(0, 2)) >= 76) {
						log.error("[PXGLibcClean] RSS 过大: {}k, 建议把一些RocketMQ线程调低或者其他内存调低给堆外腾出空间", beforeValue);
					}
				}
			}
		} catch (Throwable t) {
			log.warn("[PXGLibcClean] 提取 RSS 值时发生异常:", t);
		}
    }


    /**
     * 获取容器 memory limit（单位 KB）
     * 优先读取 cgroup v2，回退到 cgroup v1
     *
     * cgroup v2: /sys/fs/cgroup/memory.max（值为字节数，或 "max" 表示无限制）
     * cgroup v1: /sys/fs/cgroup/memory/memory.limit_in_bytes（值为字节数）
     *
     * @return memory limit（KB），获取失败或无限制时返回 -1
     */
    private long getContainerMemoryLimitKB() {
        // 先尝试 cgroup v2
        long limitKB = readCgroupMemoryLimit(CGROUP_V2_MEMORY_MAX);
        if (limitKB > 0) {
            return limitKB;
        }
        // 回退到 cgroup v1
        limitKB = readCgroupMemoryLimit(CGROUP_V1_MEMORY_LIMIT);
        if (limitKB > 0) {
            return limitKB;
        }
        log.warn("[PXGLibcClean] 无法获取容器 memory limit，cgroup 文件不存在或不可读");
        return -1;
    }

    /**
     * 读取 cgroup memory limit 文件
     *
     * @param filePath cgroup 文件路径
     * @return memory limit（KB），获取失败或无限制时返回 -1
     */
    private long readCgroupMemoryLimit(String filePath) {
        try {
            java.nio.file.Path path = java.nio.file.Paths.get(filePath);
            if (!java.nio.file.Files.exists(path)) {
                return -1;
            }
            String content = java.nio.file.Files.readString(path).trim();
            // cgroup v2 中 "max" 表示无限制
            if ("max".equalsIgnoreCase(content)) {
                log.info("[PXGLibcClean] cgroup memory limit 为 max（无限制）: {}", filePath);
                return -1;
            }
            long bytes = Long.parseLong(content);
            // cgroup v1 中超大值（如接近 Long.MAX_VALUE）也表示无限制
            // 通常 cgroup v1 无限制时值为 9223372036854771712
            if (bytes >= Long.MAX_VALUE / 2) {
                log.info("[PXGLibcClean] cgroup memory limit 无限制: {}", filePath);
                return -1;
            }
            return bytes / 1024; // 转换为 KB
        } catch (Throwable t) {
            log.warn("[PXGLibcClean] 读取 cgroup memory limit 失败: {}", filePath, t);
            return -1;
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
