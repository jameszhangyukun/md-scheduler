package com.md.scheduler.job.core.thread;

import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.biz.model.TriggerParam;
import com.md.scheduler.job.core.handler.IJobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * 定时任务执行线程
 */
public class JobThread extends Thread {
    private static Logger logger = LoggerFactory.getLogger(JobThread.class);

    /**
     * 任务id
     */
    private int jobId;
    /**
     *
     */
    private IJobHandler handler;
    /**
     * 触发器参数队列
     */
    private LinkedBlockingDeque<TriggerParam> triggerQueue;
    /**
     * 线程终止的标记
     */
    private volatile boolean toStop = false;
    /**
     * 线程终止的原因
     */
    private String stopReason;
    /**
     * 线程是否正在运行
     */
    private boolean running = false;
    /**
     * 线程空闲的时间
     */
    private int idleTimes = 0;

    public JobThread(int jobId, IJobHandler handler) {
        this.jobId = jobId;
        this.handler = handler;
        this.triggerQueue = new LinkedBlockingDeque<>();
        this.setName("job jobThread-" + jobId + "-" + System.currentTimeMillis());
    }

    public IJobHandler getJobHandler() {
        return handler;
    }

    /**
     * 触发器参数放入队列
     */
    public ReturnT<String> pushTriggerQueue(TriggerParam triggerParam) {
        triggerQueue.offer(triggerParam);
        return ReturnT.SUCCESS;
    }

    /**
     * 终止线程
     *
     * @param stopReason
     */
    public void toStop(String stopReason) {
        this.toStop = true;
        this.stopReason = stopReason;
    }

    /**
     * 判断线程是否有任务，并且正在运行
     *
     * @return
     */
    public boolean isRunningOrHasQueue() {
        return running || !triggerQueue.isEmpty();
    }

    @Override
    public void run() {
        try {
            // 如果IJobHandler中封装了bean对象的初始化方法，并且该定时任务注解中声明了初始化方法要执行
            handler.init();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        while (!toStop) {
            // 从队列中不断取出待执行的定时任务
            // TODO
        }
    }
}
