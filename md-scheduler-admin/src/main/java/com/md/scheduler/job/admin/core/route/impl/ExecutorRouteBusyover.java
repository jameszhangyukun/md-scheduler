package com.md.scheduler.job.admin.core.route.impl;

import com.md.scheduler.job.admin.core.route.ExecutorRouter;
import com.md.scheduler.job.admin.core.scheduler.JobScheduler;
import com.md.scheduler.job.admin.core.util.I18nUtil;
import com.md.scheduler.job.core.biz.ExecutorBiz;
import com.md.scheduler.job.core.biz.model.IdleBeatParam;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.biz.model.TriggerParam;

import java.util.List;

/**
 * 忙碌转移策略
 */
public class ExecutorRouteBusyover extends ExecutorRouter {

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        StringBuffer stringBuffer = new StringBuffer();
        for (String address : addressList) {
            ReturnT<String> idleBeatResult = null;
            try {
                // 得到向执行器发送消息的客户端
                ExecutorBiz executorBiz = JobScheduler.getExecutorBiz(address);
                // 向客户端发送忙碌检测请求，判断该执行器的定时任务线程是否正在执行对应的定时任务
                // 如果正在执行，说明比较忙碌，则不使用该地址
                executorBiz.idleBeat(new IdleBeatParam(triggerParam.getJobId()));
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                idleBeatResult = new ReturnT<>(ReturnT.FAIL_CODE, "" + e);
            }
            stringBuffer.append((stringBuffer.length() > 0) ? "<br><br>" : "")
                    .append(I18nUtil.getString("jobconf_idleBeat") + "：")
                    .append("<br>address：").append(address)
                    .append("<br>code：").append(idleBeatResult.getCode())
                    .append("<br>msg：").append(idleBeatResult.getMsg());
            //如果不忙碌就直接使用该地址
            if (idleBeatResult.getCode() == ReturnT.SUCCESS_CODE) {
                idleBeatResult.setMsg(stringBuffer.toString());
                idleBeatResult.setContent(address);
                return idleBeatResult;
            }
        }
        return new ReturnT<>(ReturnT.FAIL_CODE, stringBuffer.toString());
    }
}
