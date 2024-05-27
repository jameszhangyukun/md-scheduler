package com.md.scheduler.job.core.biz.impl;

import com.md.scheduler.job.core.biz.ExecutorBiz;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.biz.model.TriggerParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 执行器进行定时调用
 */
public class ExecutorBizImpl implements ExecutorBiz {

    private static Logger logger = LoggerFactory.getLogger(ExecutorBizImpl.class);

    /**
     * 执行定时任务的方法
     */
    @Override
    public ReturnT<String> run(TriggerParam triggerParam) {
        // TODO 执行定时任务的run方法
        return null;
    }
}
