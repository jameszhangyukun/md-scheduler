package com.md.scheduler.job.admin.core.thread;

import com.md.scheduler.job.admin.core.conf.JobAdminConfig;
import com.md.scheduler.job.core.biz.model.RegistryParam;
import com.md.scheduler.job.core.biz.model.ReturnT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.concurrent.*;

/**
 * 注册执行器的线程池
 */
public class JobRegistryHelper {

    private static Logger logger = LoggerFactory.getLogger(JobRegistryHelper.class);

    private static JobRegistryHelper instance = new JobRegistryHelper();

    public static JobRegistryHelper getInstance() {
        return instance;
    }

    /**
     * 注册或者移除执行器地址
     */
    private ThreadPoolExecutor registryOrRemoveThreadPool;

    private volatile boolean toStop = false;

    public void start() {
        registryOrRemoveThreadPool = new ThreadPoolExecutor(
                2,
                10,
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "job, admin JobRegistryMonitorHelper-registryOrRemoveThreadPool-" + r.hashCode());
                    }
                },
                //下面这个是xxl-job定义的线程池拒绝策略，其实就是把被拒绝的任务再执行一遍
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        //在这里能看到，所谓的拒绝，就是把任务再执行一遍
                        r.run();
                        logger.warn(">>>>>>>>>>> xxl-job, registry or remove too fast, match threadpool rejected handler(run now).");
                    }
                });
    }

    public void toStop() {
        toStop = true;
        registryOrRemoveThreadPool.shutdown();
    }

    /**
     * 执行器注册方法
     */
    public ReturnT<String> registry(RegistryParam registryParam) {
        if (!StringUtils.hasText(registryParam.getRegistryGroup())
                || !StringUtils.hasText(registryParam.getRegistryKey())
                || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }
        registryOrRemoveThreadPool.execute(() -> {
            //
            int save = JobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registryUpdate(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue(), new Date());
            if (save < 1) {
                //数据库中没有相应数据，直接新增即可
                JobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registrySave(
                        registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue(), new Date());
                //刷新注册表信息的
                freshGroupRegistryInfo(registryParam);
            }

        });
        return ReturnT.SUCCESS;
    }

    public ReturnT<String> registryRemove(RegistryParam registryParam) {
        if (!StringUtils.hasText(registryParam.getRegistryGroup())
                || !StringUtils.hasText(registryParam.getRegistryKey())
                || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }
        registryOrRemoveThreadPool.execute(() -> {
            int ret = JobAdminConfig.getAdminConfig().getXxlJobRegistryDao()
                    .registryDelete(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue());

            if (ret > 0) {
                freshGroupRegistryInfo(registryParam);
            }
        });
        return ReturnT.SUCCESS;
    }


    // TODO
    private void freshGroupRegistryInfo(RegistryParam registryParam) {
    }
}
