package com.md.scheduler.job.core.thread;

import com.md.scheduler.job.core.biz.AdminBiz;
import com.md.scheduler.job.core.biz.model.RegistryParam;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.enums.RegistryConfig;
import com.md.scheduler.job.core.executor.JobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 远程注册
 */
public class ExecutorRegistryThread {

    private static Logger logger = LoggerFactory.getLogger(ExecutorRegistryThread.class);

    private static ExecutorRegistryThread instance = new ExecutorRegistryThread();

    private Thread registryThread;

    public static ExecutorRegistryThread getInstance() {
        return instance;
    }

    private volatile boolean toStop = false;

    public void start(final String appName, final String address) {
        if (appName == null || appName.trim().length() == 0) {
            logger.warn(">>>>>>>>>>> job, executor registry config fail, appname is null.");
            return;
        }
        if (JobExecutor.getAdminBizList() == null || JobExecutor.getAdminBizList().size() == 0) {
            logger.warn(">>>>>>>>>>> job, executor registry config fail, adminAddresses is null.");
            return;
        }
        this.registryThread = new Thread(() -> {
            // 执行注册逻辑
            while (!toStop) {
                try {
                    RegistryParam registryParam = new RegistryParam(RegistryConfig.RegistryType.EXECUTOR.name(), appName, address);
                    for (AdminBiz adminBiz : JobExecutor.getAdminBizList()) {
                        try {
                            ReturnT<String> registryResult = adminBiz.registry(registryParam);
                            if (registryResult != null && ReturnT.SUCCESS_CODE == registryResult.getCode()) {
                                registryResult = ReturnT.SUCCESS;
                                logger.debug(">>>>>>>>>>> job registry success, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
                                break;
                            } else {
                                //如果注册失败了，就寻找下一个调度中心继续注
                                logger.info(">>>>>>>>>>> job registry fail, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
                            }
                        } catch (Exception e) {
                            logger.error(">>>>>>>>>>> job registry error, registryParam:{}", registryParam, e);
                        }
                    }
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                try {
                    if (!toStop) {
                        //这里是每隔30秒，就再次循环重新注册一次，也就是维持心跳信息。
                        TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                    }
                } catch (InterruptedException e) {
                    if (!toStop) {
                        logger.warn(">>>>>>>>>>> xxl-job, executor registry thread interrupted, error msg:{}", e.getMessage());
                    }
                }
            }
            try {

                // 执行到这里，意味着工作线程要结束工作了，不再注册执行器
                // 需要上报信息，移除执行器
                RegistryParam registryParam = new RegistryParam(RegistryConfig.RegistryType.EXECUTOR.name(), appName, address);
                List<AdminBiz> adminBizList = JobExecutor.getAdminBizList();
                for (AdminBiz adminBiz : adminBizList) {
                    try {
                        ReturnT<String> registryResult = adminBiz.registryRemove(registryParam);
                        if (registryResult != null && ReturnT.SUCCESS_CODE == registryResult.getCode()) {
                            registryResult = ReturnT.SUCCESS;
                            logger.info(">>>>>>>>>>> job registry-remove success, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
                            break;
                        } else {
                            logger.info(">>>>>>>>>>> job registry-remove fail, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
                        }
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.info(">>>>>>>>>>>job registry-remove error, registryParam:{}", registryParam, e);
                        }
                    }
                }
            } catch (Exception e) {
                if (!toStop) {
                    logger.error(e.getMessage(), e);
                }
            }
        });
        registryThread.setDaemon(true);
        registryThread.setName("md-scheduler-registry-thread");
        registryThread.start();
    }
}
