package com.md.scheduler.job.admin.service.impl;

import com.md.scheduler.job.admin.core.thread.JobRegistryHelper;
import com.md.scheduler.job.core.biz.AdminBiz;
import com.md.scheduler.job.core.biz.model.RegistryParam;
import com.md.scheduler.job.core.biz.model.ReturnT;
import org.springframework.stereotype.Service;

/**
 * 调度器注册组件
 */
@Service
public class AdminBizImpl implements AdminBiz {
    @Override
    public ReturnT<String> registry(RegistryParam registryParam) {
        return JobRegistryHelper.getInstance().registry(registryParam);
    }

    @Override
    public ReturnT<String> registryRemove(RegistryParam registryParam) {
        return JobRegistryHelper.getInstance().registryRemove(registryParam);
    }
}
