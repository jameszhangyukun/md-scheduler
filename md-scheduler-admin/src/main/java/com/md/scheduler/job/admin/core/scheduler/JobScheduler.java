package com.md.scheduler.job.admin.core.scheduler;

import com.md.scheduler.job.admin.core.conf.JobAdminConfig;
import com.md.scheduler.job.admin.core.thread.JobRegistryHelper;
import com.md.scheduler.job.admin.core.thread.JobScheduleHelper;
import com.md.scheduler.job.admin.core.thread.JobTriggerPoolHelper;
import com.md.scheduler.job.core.biz.ExecutorBiz;
import com.md.scheduler.job.core.biz.client.ExecutorBizClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * job服务单的启动类，在init方法中初始化各个组件
 */
public class JobScheduler {

    private static final Logger logger = LoggerFactory.getLogger(JobScheduler.class);

    /**
     * 初始化
     */
    public void init() throws Exception {
        // 触发创建线程池，包括快线程池、慢线程池
        JobTriggerPoolHelper.toStart();
        // 初始化注册中心版本
        JobRegistryHelper.getInstance().start();
        // 初始化任务调度线程
        // 时间轮是通过线程+容器组合形成
        JobScheduleHelper.getInstance().start();
    }

    public void destroy() throws Exception {
        JobTriggerPoolHelper.toStop();
        JobRegistryHelper.getInstance().toStop();
        JobScheduleHelper.getInstance().toStop();
    }

    /**
     * 远程调用客户端客户端集合
     * key是远程调用的地址，Value是远程调用的客户端
     */
    private static ConcurrentHashMap<String, ExecutorBiz> executorBizMap = new ConcurrentHashMap<>();

    /**
     * 获取进行远程调用的客户端，
     */
    public static ExecutorBiz getExecutorBiz(String address) {
        if (address == null || address.trim().length() == 0) {
            return null;
        }
        // 去掉地址空格
        address = address.trim();
        // 从远程调用的Map集合中获得远程调用的客户端
        ExecutorBiz executorBiz = executorBizMap.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }

        executorBiz = new ExecutorBizClient(address, JobAdminConfig.getAdminConfig().getAccessToken());
        executorBizMap.put(address, executorBiz);
        return executorBiz;
    }
}
