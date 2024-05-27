package com.md.scheduler.job.core.handler.annotation;

/**
 * Job注解
 */
public @interface Job {
    /**
     * 任务名称
     *
     * @return
     */
    String value();

    /**
     * 任务初始化方法
     *
     * @return
     */
    String init() default "";

    /**
     * 销毁方法
     *
     * @return
     */
    String destroy() default "";
}
