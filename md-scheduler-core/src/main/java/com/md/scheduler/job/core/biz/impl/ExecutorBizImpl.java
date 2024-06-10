package com.md.scheduler.job.core.biz.impl;

import com.md.scheduler.job.core.biz.ExecutorBiz;
import com.md.scheduler.job.core.biz.model.IdleBeatParam;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.biz.model.TriggerParam;
import com.md.scheduler.job.core.executor.JobExecutor;
import com.md.scheduler.job.core.glue.GlueTypeEnum;
import com.md.scheduler.job.core.handler.IJobHandler;
import com.md.scheduler.job.core.thread.JobThread;
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
        // 通过任务id，取出执行线程
        JobThread jobThread = JobExecutor.loadJobThread(triggerParam.getJobId());
        IJobHandler jobHandler = jobThread != null ? jobThread.getJobHandler() : null;
        String removeOldReason = null;
        // 获取定时任务的调度模式
        GlueTypeEnum glueTypeEnum = GlueTypeEnum.match(triggerParam.getGlueType());
        // 如果是bean模式，就通过定时器的名称，从jobHandlerRepository中获取jobHandler
        if (glueTypeEnum == GlueTypeEnum.BEAN) {
            IJobHandler iJobHandler = JobExecutor.loadJobHandler(triggerParam.getExecutorHandler());
            // 如果任务不为空，说明定时任务已经执行，并分配了对应的执行线程
            // 但是根据定时任务名称和从jobHandlerRepository中得到封装的定时任务方法的对象不同
            // 说明定时任务已经改变
            if (jobHandler != null && jobHandler != iJobHandler) {
                removeOldReason = "change job handler or glue type and terminate the old job thread.";
                jobThread = null;
                jobHandler = null;
            }
            if (jobHandler == null) {
                // 说明jobHandler为null
                // 此时调度任务是第一次执行
                jobHandler = iJobHandler;
                if (jobHandler == null) {
                    // 如果此时还是空，说明执行器没有对应的定时任务
                    return new ReturnT<>(ReturnT.FAIL_CODE, "job handler [" + triggerParam.getExecutorHandler() + "] not found.");
                }
            }
        } else {
            //如果没有合适的调度模式，就返回调用失败的信息
            return new ReturnT<String>(ReturnT.FAIL_CODE, "glueType[" + triggerParam.getGlueType() + "] is not valid.");
        }
        if (jobThread == null) {
            //走到这里意味着定时任务是第一次执行，还没有创建对应的执行定时任务的线程，所以，就在这里把对应的线程创建出来，
            //并且缓存到jobThreadRepository这个Map中
            //在这里就用到了上面赋值过的jobHandler
            jobThread = JobExecutor.registerJobThread(triggerParam.getJobId(), jobHandler, removeOldReason);
        }
        //如果走到这里，不管上面是什么情况吧，总之jobThread肯定存在了，所以直接把要调度的任务放到这个线程内部的队列中
        //等待线程去调用，返回一个结果
        ReturnT<String> pushResult = jobThread.pushTriggerQueue(triggerParam);
        return pushResult;
    }

    @Override
    public ReturnT<String> idleBeat(IdleBeatParam idleBeatParam) {
        boolean isRunningOrhasQueue = false;
        JobThread jobThread = JobExecutor.loadJobThread(idleBeatParam.getJobId());
        if (jobThread != null && jobThread.isRunningOrHasQueue()) {
            isRunningOrhasQueue = true;
        }
        if (isRunningOrhasQueue) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "job thread is running or has trigger queue.");
        }
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> beat() {
        return ReturnT.SUCCESS;
    }
}
