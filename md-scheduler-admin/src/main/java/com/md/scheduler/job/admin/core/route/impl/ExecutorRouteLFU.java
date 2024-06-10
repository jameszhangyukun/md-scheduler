package com.md.scheduler.job.admin.core.route.impl;

import com.md.scheduler.job.admin.core.route.ExecutorRouter;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.biz.model.TriggerParam;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 最不经常使用的路由策略，频率/次数
 */
public class ExecutorRouteLFU extends ExecutorRouter {

    private static ConcurrentHashMap<Integer, HashMap<String, Integer>> jobLfuMap = new ConcurrentHashMap<>();

    private static long CACHE_VALID_TIME = 0;

    public String route(int jobId, List<String> addressList) {
        // 判断当前时间是否大于Map的缓存时间
        if (System.currentTimeMillis() > CACHE_VALID_TIME) {
            // 如果超过缓的有效事件，清除缓存即可
            jobLfuMap.clear();
            // 重新设置有效期
            CACHE_VALID_TIME = System.currentTimeMillis() + 100 * 60 * 60 * 24;
        }

        HashMap<String, Integer> lfuItemMap = jobLfuMap.get(jobId);
        if (lfuItemMap == null) {
            lfuItemMap = new HashMap<>();
            jobLfuMap.putIfAbsent(jobId, lfuItemMap);
        }

        for (String address : addressList) {
            if (!lfuItemMap.containsKey(address) || lfuItemMap.get(address) > 1000000) {
                lfuItemMap.put(address, new Random().nextInt(addressList.size()));
            }
        }

        //判断有没有过期的执行器
        List<String> delKeys = new ArrayList<>();
        for (String existKey : lfuItemMap.keySet()) {
            if (!addressList.contains(existKey)) {
                delKeys.add(existKey);
            }
        }
        //如果有就把过期的执行器从lfuItemMap中移除
        if (delKeys.size() > 0) {
            for (String delKey : delKeys) {
                lfuItemMap.remove(delKey);
            }
        }
        //下面就开始选择具体的执行器来执行定时任务了，把lfuItemMap中的数据转移到lfuItemList中
        List<Map.Entry<String, Integer>> lfuItemList = new ArrayList<Map.Entry<String, Integer>>(lfuItemMap.entrySet());
        //将lfuItemList中的数据按照执行器的使用次数做排序
        Collections.sort(lfuItemList, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });
        //获取到的第一个就是使用次数最少的执行器
        Map.Entry<String, Integer> addressItem = lfuItemList.get(0);
        String minAddress = addressItem.getKey();
        //因为要是用它了，所以把执行器的使用次数加1
        addressItem.setValue(addressItem.getValue() + 1);
        //返回执行器地址
        return addressItem.getKey();

    }

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        String address = route(triggerParam.getJobId(), addressList);
        return new ReturnT<String>(address);
    }
}
