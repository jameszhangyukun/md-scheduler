package com.md.scheduler.job.core.handler;

/**
 * 定时任务方法的接口
 */
public abstract class IJobHandler {
    public abstract void execute() throws Exception;

    public void init() throws Exception {

    }

    public void destroy() throws Exception {

    }
}
