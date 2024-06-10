package com.md.scheduler.job.admin.core.route.impl;

import com.md.scheduler.job.admin.core.route.ExecutorRouter;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.biz.model.TriggerParam;

import java.util.List;

/**
 * 集合中最后一个地址
 */
public class ExecutorRouteLast extends ExecutorRouter {
    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        return new ReturnT<String>(addressList.get(addressList.size() - 1));
    }
}
