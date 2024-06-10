package com.md.scheduler.job.admin.core.route.impl;


import com.md.scheduler.job.admin.core.route.ExecutorRouter;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.biz.model.TriggerParam;

import java.util.List;
import java.util.Random;

/**
 * 随机选择一个执行器地址
 */
public class ExecutorRouteRandom extends ExecutorRouter {

    private static Random localRandom = new Random();

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        String address = addressList.get(localRandom.nextInt(addressList.size()));
        return new ReturnT<String>(address);
    }
}
