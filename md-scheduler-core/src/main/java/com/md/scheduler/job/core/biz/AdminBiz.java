package com.md.scheduler.job.core.biz;

import com.md.scheduler.job.core.biz.model.RegistryParam;
import com.md.scheduler.job.core.biz.model.ReturnT;

/**
 * 程序内部使用的接口，调度中心暴露给执行器
 */
public interface AdminBiz {
    /**
     * 执行器注册到调度中心
     */
    ReturnT<String> registry(RegistryParam registryParam);

    /**
     * 执行器从调度中心移除
     */
    ReturnT<String> registryRemove(RegistryParam registryParam);
}
