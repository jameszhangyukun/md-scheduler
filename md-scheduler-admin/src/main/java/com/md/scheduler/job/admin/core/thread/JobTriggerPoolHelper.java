package com.md.scheduler.job.admin.core.thread;

import com.md.scheduler.job.admin.core.conf.JobAdminConfig;
import com.md.scheduler.job.admin.core.trigger.JobTrigger;
import com.md.scheduler.job.admin.core.trigger.TriggerTypeEnum;
import com.md.scheduler.job.core.biz.model.TriggerParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.Trigger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务触发线程池，由线程池执行或触发任务
 */
public class JobTriggerPoolHelper {


    private static Logger logger = LoggerFactory.getLogger(JobTriggerPoolHelper.class);
    /**
     * 快线程池
     */
    private ThreadPoolExecutor fastTriggerPool = null;
    /**
     * 慢线程池
     */
    private ThreadPoolExecutor slowTriggerPool = null;

    public void start() {
        fastTriggerPool = new ThreadPoolExecutor(
                10,
                JobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(1000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "job, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode());
                    }
                }
        );


        slowTriggerPool = new ThreadPoolExecutor(
                10,
                JobAdminConfig.getAdminConfig().getTriggerPoolSlowMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "job, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode());
                    }
                });

    }

    /**
     * 关闭线程池
     */
    public void stop() {
        fastTriggerPool.shutdown();
        slowTriggerPool.shutdown();
        logger.info("job trigger thread pool shutdown success.");
    }

    /**
     * 系统当前的分钟数
     */
    private volatile long minTime = System.currentTimeMillis() / 60000;
    /**
     * 记录慢任务的情况
     * key为JobId，value为慢执行的次数
     */
    private volatile ConcurrentHashMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();


    private static JobTriggerPoolHelper helper = new JobTriggerPoolHelper();

    public void addTrigger(final int jobId,
                           final TriggerTypeEnum triggerTypeEnum,
                           final int failRetryCount,
                           final String executorShardingParma,
                           final String executorParam,
                           final String addressList) {
        ThreadPoolExecutor triggerPool = fastTriggerPool;
        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
        if (jobTimeoutCount != null && jobTimeoutCount.get() > 10) {
            triggerPool = slowTriggerPool;
        }
        triggerPool.execute(() -> {
            long start = System.currentTimeMillis();
            try {
                // 调用触发器执行任务 调用远程调用
                JobTrigger.trigger(jobId, triggerTypeEnum, failRetryCount, executorShardingParma, executorParam, addressList);

            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
                long minTimeNow = System.currentTimeMillis() / 60000;
                // 如果分钟数，则一定是经过了1分钟，所以清空map
                if (minTime != minTimeNow) {
                    minTime = minTimeNow;
                    jobTimeoutCountMap.clear();
                }
                long cost = System.currentTimeMillis() - start;
                if (cost > 500) {
                    AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                    if (timeoutCount != null) {
                        // 慢执行次数加一
                        timeoutCount.incrementAndGet();
                    }
                }
            }
        });
    }

    /**
     * 启动方法
     */
    public static void toStart() {
        helper.start();
    }

    /**
     * 停止方法
     */
    public static void toStop() {
        helper.stop();
    }

    /**
     * 添加触发器
     */
    public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount, String executorShardingParam,
                               String executorParam, String addressList) {
        helper.addTrigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
    }
}
