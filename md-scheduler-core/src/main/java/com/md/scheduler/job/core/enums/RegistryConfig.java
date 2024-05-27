package com.md.scheduler.job.core.enums;

public class RegistryConfig {
    /**
     * 执行器心跳
     */
    public static final int BEAT_TIMEOUT = 30;

    public static final int DEAD_TIMEOUT = BEAT_TIMEOUT * 3;

    /**
     * 注册类型
     */
    public enum RegistryType {
        EXECUTOR, ADMIN
    }
}
