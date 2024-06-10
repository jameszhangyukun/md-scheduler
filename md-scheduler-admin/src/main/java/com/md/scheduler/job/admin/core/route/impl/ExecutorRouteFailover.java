package com.md.scheduler.job.admin.core.route.impl;

import com.md.scheduler.job.admin.core.route.ExecutorRouter;
import com.md.scheduler.job.admin.core.scheduler.JobScheduler;
import com.md.scheduler.job.admin.core.util.I18nUtil;
import com.md.scheduler.job.core.biz.ExecutorBiz;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.biz.model.TriggerParam;

import java.util.List;

/**
 * 故障转移策略
 */
public class ExecutorRouteFailover extends ExecutorRouter {

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        StringBuffer beatResultSB = new StringBuffer();
        //遍历得到的执行器地址
        for (String address : addressList) {
            ReturnT<String> beatResult = null;
            try {
                //得到访问执行器的客户端
                ExecutorBiz executorBiz = JobScheduler.getExecutorBiz(address);
                //向执行器发送心跳检测请求，看执行器是否还在线
                beatResult = executorBiz.beat();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                beatResult = new ReturnT<String>(ReturnT.FAIL_CODE, "" + e);
            }
            beatResultSB.append((beatResultSB.length() > 0) ? "<br><br>" : "")
                    .append(I18nUtil.getString("jobconf_beat") + "：")
                    .append("<br>address：").append(address)
                    .append("<br>code：").append(beatResult.getCode())
                    .append("<br>msg：").append(beatResult.getMsg());
            //心跳检测没问题，就直接使用该执行器
            if (beatResult.getCode() == ReturnT.SUCCESS_CODE) {

                beatResult.setMsg(beatResultSB.toString());
                beatResult.setContent(address);
                return beatResult;
            }
        }
        return new ReturnT<>(ReturnT.FAIL_CODE, beatResultSB.toString());
    }
}
