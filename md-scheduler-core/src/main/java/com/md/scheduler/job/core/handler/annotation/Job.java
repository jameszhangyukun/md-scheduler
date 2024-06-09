package com.md.scheduler.job.core.handler.annotation;

import java.lang.annotation.*;

/**
 * Job注解
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
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
