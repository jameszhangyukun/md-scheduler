package com.md.scheduler.job.admin.core.thread;

import com.md.scheduler.job.admin.core.conf.JobAdminConfig;
import com.md.scheduler.job.admin.core.model.JobGroup;
import com.md.scheduler.job.admin.core.model.JobRegistry;
import com.md.scheduler.job.core.biz.model.RegistryParam;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.enums.RegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.*;
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

    private Thread registryMonitorThread;


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


        // 启动线程循环检测注册中心的执行器是否过期，如果过期就移除数据
        // 用来进行心跳检测的作用，该线程每次循环都会休眠30秒
        registryMonitorThread = new Thread(() -> {
            while (!toStop) {
                try {
                    // 查询所有自动注册的执行器组
                    List<JobGroup> jobGroups = JobAdminConfig.getAdminConfig().getXxlJobGroupDao().findByAddressType(0);

                    if (jobGroups != null && !jobGroups.isEmpty()) {
                        // 去DB中查询执行器最后的更新时间是否小于当前时间-90秒，如果小于就表示超时
                        // 在90秒内，执行器没有更新自己的信息，表示该执行器宕机
                        List<Integer> ids = JobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findDead(RegistryConfig.DEAD_TIMEOUT, new Date());
                        if (ids != null && !ids.isEmpty()) {
                            // 删除过期的执行器
                            JobAdminConfig.getAdminConfig().getXxlJobRegistryDao().removeDead(ids);
                        }
                        // 缓存appname和执行器地址的映射
                        HashMap<String, List<String>> appAddressMap = new HashMap<>();

                        // 查询过去90秒的执行器
                        List<JobRegistry> list = JobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findAll(RegistryConfig.DEAD_TIMEOUT, new Date());
                        if (list != null && !list.isEmpty()) {
                            for (JobRegistry jobRegistry : list) {
                                // 自动注册
                                if (RegistryConfig.RegistryType.EXECUTOR.name().equals(jobRegistry.getRegistryGroup())) {
                                    String appname = jobRegistry.getRegistryKey();
                                    List<String> registryList = appAddressMap.get(appname);
                                    if (registryList == null) {
                                        registryList = new ArrayList<>();
                                    }
                                    if (!registryList.contains(jobRegistry.getRegistryValue())) {
                                        registryList.add(jobRegistry.getRegistryValue());
                                    }
                                    appAddressMap.put(appname, registryList);
                                }
                            }
                        }


                        for (JobGroup group : jobGroups) {
                            List<String> registryList = appAddressMap.get(group.getAppname());
                            String addressListStr = null;
                            if (registryList != null && !registryList.isEmpty()) {
                                Collections.sort(registryList);
                                StringBuilder addressListSB = new StringBuilder();
                                for (String item:registryList) {
                                    addressListSB.append(item).append(",");
                                }
                                addressListStr = addressListSB.toString();
                                addressListStr = addressListStr.substring(0, addressListStr.length()-1);
                            }
                            group.setAddressList(addressListStr);
                            group.setUpdateTime(new Date());
                            JobAdminConfig.getAdminConfig().getXxlJobGroupDao().update(group);
                        }
                    }
                }catch (Exception e){
                    if (!toStop) {
                        logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:", e);
                    }
                }
                try {
                    //线程在这里睡30秒，也就意味着检测周期为30秒
                    TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                } catch (InterruptedException e) {
                    if (!toStop) {
                        logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:", e);
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, job registry monitor thread stop");
            }
        });

        registryMonitorThread.setDaemon(true);
        registryMonitorThread.setName("xxl-job, admin JobRegistryMonitorHelper-registryMonitorThread");
        registryMonitorThread.start();
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
