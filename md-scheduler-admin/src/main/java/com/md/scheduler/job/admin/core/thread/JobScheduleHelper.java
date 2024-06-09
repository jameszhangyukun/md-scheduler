package com.md.scheduler.job.admin.core.thread;


import com.md.scheduler.job.admin.core.conf.JobAdminConfig;
import com.md.scheduler.job.admin.core.cron.CronExpression;
import com.md.scheduler.job.admin.core.model.JobInfo;
import com.md.scheduler.job.admin.core.scheduler.MisfireStrategyEnum;
import com.md.scheduler.job.admin.core.scheduler.ScheduleTypeEnum;
import com.md.scheduler.job.admin.core.trigger.TriggerTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 任务调度执行，不停扫描DB，获取可执行的任务
 */
public class JobScheduleHelper {

    private static Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);

    private static JobScheduleHelper instance = new JobScheduleHelper();

    public static JobScheduleHelper getInstance() {
        return instance;
    }

    public static final long PRE_READ_MS = 5000;

    private Thread scheduleThread;

    private Thread ringThread;

    private volatile boolean scheduleThreadToStop = false;

    private volatile boolean ringThreadToStop = false;

    private volatile static Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();


    public void start() {
        scheduleThread = new Thread(() -> {
            try {
                // 对齐整数秒，如果现在不是5秒的整数，则休眠到整数。
                TimeUnit.SECONDS.sleep(5000 - System.currentTimeMillis() % 1000);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            logger.info(">>>>>>>>>>>>>>> init job admin scheduler success.");
            // 默认每个定时任务大概消耗50ms，所以一秒执行20个
            // 定时任务的执行，在调度中心实际上就是快慢线程执行触发任务，定时任务的执行在执行器端
            // 如果快慢线程都满了，此时总共是300线程，每秒是20个任务，每秒最多可以调度6000个定时任务
            // 6000是一个限制值，如果从DB中去最多每秒可以取出6000个，主要是配合线程池
            int preReadCount = (JobAdminConfig.getAdminConfig().getTriggerPoolFastMax() + JobAdminConfig.getAdminConfig().getTriggerPoolSlowMax()) * 20;

            while (!scheduleThreadToStop) {
                // 调度任务的开始时间
                long start = System.currentTimeMillis();
                // 依赖数据库实现分布式锁
                Connection connection = null;
                Boolean autoCommit = null;
                PreparedStatement preparedStatement = null;
                boolean preReadSuc = true;
                try {
                    connection = JobAdminConfig.getAdminConfig().getDataSource().getConnection();
                    autoCommit = connection.getAutoCommit();
                    connection.setAutoCommit(false);
                    preparedStatement = connection.prepareStatement("select * from xxl_job_lock where lock_name = 'schedule_lock' for update ");
                    preparedStatement.execute();

                    long nowTime = System.currentTimeMillis();
                    List<JobInfo> jobInfos = JobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);

                    if (!CollectionUtils.isEmpty(jobInfos)) {
                        for (JobInfo jobInfo : jobInfos) {
                            // 判断当前时间是不是大于任务的下一次执行时间在加上5秒
                            // 如果服务器宕机，就会导致本来上一次要执行的任务，没有执行
                            // 比如任务在第5秒执行，但是第4秒系统宕机，然后12秒重启
                            if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS) {
                                logger.warn(">>>>>>>>> job schedule misfire, jobId = " + jobInfo.getId());
                                MisfireStrategyEnum misfireStrategyEnum = MisfireStrategyEnum.match(jobInfo.getMisfireStrategy(), MisfireStrategyEnum.DO_NOTHING);
                                // 如果此时的策略是，失败重试，立即执行一次任务
                                if (MisfireStrategyEnum.FIRE_ONCE_NOW == misfireStrategyEnum) {
                                    JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.MISFIRE, -1, null, null, null);
                                    // 执行一次任务
                                    logger.debug(">>>>>>>>>>> job, schedule push trigger : jobId = {}", jobInfo.getId());
                                }
                                // 把过期任务的下一次执行时间刷新，
                                refreshNextValidTime(jobInfo, new Date());
                            } else if (nowTime > jobInfo.getTriggerNextTime()) {
                                //比如说这个任务要在第2秒执行，但是服务器在第1秒宕机了，恢复之后已经是第4秒了，现在任务的执行时间小于了当前时间，但是仍然在5秒的调度器内
                                JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.CRON, -1,
                                        null, null, null);
                                logger.debug(">>>>>>>>>>> job, schedule push trigger : jobId = {}", jobInfo.getId());
                                refreshNextValidTime(jobInfo, new Date());
                                // 判断任务的启动状态，然后判断任务的下一次执行时间是不是小于这个执行周期。也就是这个任务会在当前调度周期内执行多次
                                // 此时直接放入到时间轮。可以避免查询数据库阻塞太久
                                if (jobInfo.getTriggerStatus() == 1 && nowTime + PRE_READ_MS > jobInfo.getTriggerNextTime()) {
                                    int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);
                                    //把定时任务的信息，就是它的id放进时间轮
                                    pushTimeRing(ringSecond, jobInfo.getId());
                                    //刷新定时任务的下一次的执行时间，注意，这里传进去的就不再是当前时间了，而是定时任务现在的下一次执行时间
                                    //因为放到时间轮中就意味着它要执行了，所以计算新的执行时间就行了
                                    refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                                }
                            } //最后，这里得到的就是最正常的任务，也就是执行时间在当前时间之后，但是又小于执行周期的任务
                            //上面的几个判断，都是当前时间大于任务的下次执行时间，实际上都是在过期的任务中做判断
                            else {
                                //这样的任务就很好处理了，反正都是调度周期，也就是当前时间5秒内要执行的任务，所以直接放到时间轮中就行
                                //计算出定时任务在时间轮中的刻度，其实就是定时任务执行的时间对应的秒数
                                //随着时间流逝，时间轮也是根据当前时间秒数来获取要执行的任务的，所以这样就可以对应上了
                                int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);
                                //放进时间轮中
                                pushTimeRing(ringSecond, jobInfo.getId());
                                //刷新定时任务下一次的执行时间
                                refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));

                            }
                        }
                        //最后再更新一下所有的任务
                        for (JobInfo jobInfo : jobInfos) {
                            JobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleUpdate(jobInfo);
                        }
                    } else {
                        // 没有扫描
                        preReadSuc = false;
                    }
                } catch (Exception e) {
                    if (!scheduleThreadToStop) {
                        logger.error(">>>>>>>>>>>>>>>>>> JobSchedulerHelper#scheduleThread error ", e);
                    }
                } finally {
                    if (connection != null) {
                        try {
                            connection.commit();
                        } catch (SQLException e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                        try {
                            connection.setAutoCommit(Boolean.TRUE.equals(autoCommit));
                        } catch (SQLException e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                    }

                    if (null != preparedStatement) {
                        try {
                            preparedStatement.close();
                        } catch (SQLException e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                    }

                }
                // 获取查询数据库和调度任务的总耗时
                long cost = System.currentTimeMillis() - start;
                if (cost < 1000) {
                    try {
                        //下面有一个三元运算，判断preReadSuc是否为true，如果扫描到数据了，就让该线程小睡一会儿，最多睡1秒
                        //如果根本就没有数据，就说明5秒的调度器内没有任何任务可以执行，那就让线程最多睡5秒，把时间睡过去，过5秒再开始工作
                        TimeUnit.MILLISECONDS.sleep((preReadSuc ? 1000 : PRE_READ_MS) - System.currentTimeMillis() % 1000);
                    } catch (InterruptedException e) {
                        if (!scheduleThreadToStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread stop");
            }
        });

        scheduleThread.setDaemon(true);
        scheduleThread.setName("job, admin JobScheduleHelper#scheduleThread");
        scheduleThread.start();

        // 时间轮工作线程
        ringThread = new Thread(() -> {
            while (!ringThreadToStop) {
                try {
                    // 时间轮存放的是每秒的任务，
                    TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis() % 1000);
                } catch (InterruptedException e) {
                    if (!scheduleThreadToStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                try {
                    // 把定时任务从集合中取出来
                    List<Integer> ringItemData = new ArrayList<>();
                    // 获取当前的时间秒数
                    int nowSecond = Calendar.getInstance().get(Calendar.SECOND);
                    // 会将未来3秒内的任务一并取出，应为如果时间轮中的某个时刻的任务太多，本来执行1秒的，然后执行了2秒，把下一个刻度跳过
                    // 所以，每次执行会把前一秒的任务也取出来，判断是否有任务
                    for (int i = 0; i < 2; i++) {
                        List<Integer> list = ringData.remove((nowSecond - i + 60) % 60);
                        if (list != null) {
                            ringItemData.addAll(list);
                        }
                    }
                    logger.debug(">>>>>>>>>>> xxl-job, time-ring beat : " + nowSecond + " = " + List.of(ringItemData));

                    if (!ringItemData.isEmpty()) {
                        for (int jobId : ringItemData) {
                            //在for循环中处理定时任务，让触发器线程池开始远程调用这些任务
                            JobTriggerPoolHelper.trigger(jobId, TriggerTypeEnum.CRON, -1, null, null, null);
                        }
                        //最后清空集合
                        ringItemData.clear();
                    }

                } catch (Exception e) {
                    if (!ringThreadToStop) {
                        logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread error ", e);
                    }
                }
            }
            logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread stop");
        });


        // 通过引入时间轮机制，来避免某些耗时严重，结束时间严重超过后续任务的执行时间，所以经常判断前面有没有未执行的任务
        ringThread.setDaemon(true);
        ringThread.setName("job, admin JobScheduleHelper#ringThread");
        ringThread.start();
    }

    /**
     * 刷新下一次定时任务的执行事件
     */
    private void refreshNextValidTime(JobInfo jobInfo, Date fromTime) throws Exception {
        Date nextValidTime = generateNextValidTime(jobInfo, fromTime);
        if (nextValidTime != null) {
            jobInfo.setTriggerNextTime(jobInfo.getTriggerNextTime());
        } else {
            jobInfo.setTriggerStatus(0);
            jobInfo.setTriggerLastTime(0);
            jobInfo.setTriggerNextTime(0);
            logger.warn(">>>>>>>>>>> job, refreshNextValidTime fail for job: jobId={}, scheduleType={}, scheduleConf={}",
                    jobInfo.getId(), jobInfo.getScheduleType(), jobInfo.getScheduleConf());
        }
    }

    /**
     * 把定时任务放入时间轮
     */
    private void pushTimeRing(int ringSecond, int jobId) {
        List<Integer> list = ringData.get(ringSecond);
        if (list == null) {
            list = new ArrayList<>();
        }
        list.add(jobId);
        ringData.put(ringSecond, list);
        logger.debug(">>>>>>>>>>> job, schedule push time-ring : " + ringSecond + " = " + Arrays.asList(list));
    }


    public void toStop() {
        scheduleThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        if (scheduleThread.getState() != Thread.State.TERMINATED) {
            scheduleThread.interrupt();
            try {
                scheduleThread.join();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        boolean hasRingData = false;
        if (!ringData.isEmpty()) {
            for (Integer second : ringData.keySet()) {
                List<Integer> list = ringData.get(second);
                if (list != null && list.size() > 0) {
                    hasRingData = true;
                    break;
                }
            }
        }
        if (hasRingData) {
            try {
                TimeUnit.SECONDS.sleep(8);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        ringThreadToStop = true;
        ringThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        if (ringThread.getState() != Thread.State.TERMINATED) {
            // interrupt and wait
            ringThread.interrupt();
            try {
                ringThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        logger.info(">>>>>>>>>>> job, JobScheduleHelper stop");
    }

    /**
     * 计算cron表达式的下一次执行时间
     */
    public static Date generateNextValidTime(JobInfo jobInfo, Date fromTime) throws Exception {
        ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
        if (ScheduleTypeEnum.CRON == scheduleTypeEnum) {
            Date nextValidTime = new CronExpression(jobInfo.getScheduleConf()).getNextValidTimeAfter(fromTime);
            return nextValidTime;
        } else if (scheduleTypeEnum == ScheduleTypeEnum.FIX_RATE) {
            return new Date(fromTime.getTime() + Integer.valueOf(jobInfo.getScheduleConf()) * 1000);
        }
        return null;
    }

}
