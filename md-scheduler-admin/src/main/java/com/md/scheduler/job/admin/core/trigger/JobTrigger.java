package com.md.scheduler.job.admin.core.trigger;

import com.md.scheduler.job.admin.core.conf.JobAdminConfig;
import com.md.scheduler.job.admin.core.model.JobGroup;
import com.md.scheduler.job.admin.core.model.JobInfo;
import com.md.scheduler.job.admin.core.route.ExecutorRouteStrategyEnum;
import com.md.scheduler.job.admin.core.scheduler.JobScheduler;
import com.md.scheduler.job.admin.core.util.I18nUtil;
import com.md.scheduler.job.core.biz.ExecutorBiz;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.biz.model.TriggerParam;
import com.md.scheduler.job.core.util.ThrowableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 任务触发
 */
public class JobTrigger {
    private static Logger logger = LoggerFactory.getLogger(JobTrigger.class);

    /**
     * 如果用户配置了执行器的地址和任务参数及分片策略，会在该方法内进行处理
     */
    public static void trigger(int jobId,
                               TriggerTypeEnum triggerType,
                               int failRetryCount,
                               String executorShardingParam,
                               String executorParam,
                               String addressList) {
        JobInfo jobInfo = JobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(jobId);
        if (jobInfo == null) {
            logger.warn(">>>>>>>>>>>>>> trigger fail, jobId invalid, jobId={}", jobId);
            return;
        }
        if (executorParam != null && !executorParam.isEmpty()) {
            jobInfo.setExecutorParam(executorParam);
        }

        //同样是根据jobId获取所有的执行器组
        JobGroup group = JobAdminConfig.getAdminConfig().getXxlJobGroupDao().load(jobInfo.getJobGroup());
        //这里也有一个小判断，如果用户在web界面输入了执行器的地址，这里会把执行器的地址设置到刚才查询到的执行器中
        //注意，这里我想强调两点，第一，这里以及上面那个设置执行器参数，都是在web界面对任务进行执行一次操作时，才会出现的调用流程
        //这个大家要弄清楚
        //第二点要强调的就是，XxlJobGroup这个对象，它并不是说里面有集合还是还是什么，在真正的生产环境中，一个定时任务不可能
        //只有一个服务器在执行吧，显然会有多个服务器在执行，对于相同的定时任务，注册到XXL-JOB的服务器上时，会把相同定时任务
        //的服务实例地址规整到一起，就赋值给XxlJobGroup这个类的addressList成员变量，不同的地址用逗号分隔即可
        if (addressList != null && !addressList.trim().isEmpty()) {
            //这里是设置执行器地址的注册方式，0是自动注册，就是1是用户手动注册的
            group.setAddressType(1);
            group.setAddressList(addressList.trim());
        }
        //执行触发器任务，这里有几个参数我直接写死了，因为现在还用不到，为了不报错，我们就直接写死
        //这里写死的都是沿用源码中设定的默认值
        //其实这里的index和total参数分别代表分片序号和分片总数的意思，但现在我们没有引入分片，只有一台执行器
        //执行定时任务，所以分片序号为0，分片总是为1。
        //分片序号代表的是执行器，如果有三个执行器，那分片序号就是0，1，2
        //分片总数就为3，这里虽然有这两个参数，实际上在第一个版本还用不到。之所以不把参数略去是因为，这样一来
        //需要改动的地方就有点多了，大家理解一下
        //在该方法内，会真正开始远程调用，这个方法，也是远程调用的核心方法
        processTrigger(group, jobInfo, -1, triggerType, 0, 1);
    }

    /**
     * 处理分片和路由策略
     */
    private static void processTrigger(JobGroup group, JobInfo jobInfo, int finalFailRetryCount, TriggerTypeEnum triggerType,
                                       int index, int total) {

        ExecutorRouteStrategyEnum executorRouteStrategyEnum = ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null);

        TriggerParam triggerParam = new TriggerParam();
        triggerParam.setJobId(jobInfo.getId());
        triggerParam.setExecutorHandler(jobInfo.getExecutorHandler());
        triggerParam.setExecutorParams(jobInfo.getExecutorParam());
        triggerParam.setGlueType(jobInfo.getGlueType());

        String address = null;
        List<String> registryList = group.getRegistryList();

        ReturnT<String> routeAddressResult = null;
        if (registryList != null && !registryList.isEmpty()) {
            routeAddressResult = executorRouteStrategyEnum.getExecutorRouter().route(triggerParam, registryList);
            if (routeAddressResult.getCode() == ReturnT.SUCCESS_CODE) {
                address = routeAddressResult.getContent();
            } else {
                routeAddressResult = new ReturnT<String>(ReturnT.FAIL_CODE, I18nUtil.getString("jobconf_trigger_address_empty"));
            }
        }

        ReturnT<String> triggerResult = null;
        // 如果地址不为空
        if (address != null) {
            triggerResult = runExecutor(triggerParam, address);
            logger.info("返回的状态码" + triggerResult.getCode());
        } else {
            triggerResult = new ReturnT<String>(ReturnT.FAIL_CODE, null);
        }

    }


    /**
     * 进行远程调用
     *
     * @param triggerParam
     * @param address
     * @return
     */
    public static ReturnT<String> runExecutor(TriggerParam triggerParam, String address) {
        ReturnT<String> runResult = null;
        try {
            ExecutorBiz executorBiz = JobScheduler.getExecutorBiz(address);
            runResult = executorBiz.run(triggerParam);
        } catch (Exception e) {
            logger.error(">>>>>>>>>>> job trigger error, please check if the executor[{}] is running.", address, e);
            runResult = new ReturnT<String>(ReturnT.FAIL_CODE, ThrowableUtil.toString(e));
        }
        //拼接远程调用返回的状态码和消息
        String runResultSB = I18nUtil.getString("jobconf_trigger_run") + "：" + "<br>address：" + address +
                "<br>code：" + runResult.getCode() +
                "<br>msg：" + runResult.getMsg();
        runResult.setMsg(runResultSB);

        return runResult;
    }


    private static boolean isNumeric(String str) {
        try {
            int result = Integer.valueOf(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
