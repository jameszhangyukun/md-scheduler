package com.md.scheduler.job.core.biz;

import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.biz.model.TriggerParam;

/**
 * 远程调用的客户端接口
 */
public interface ExecutorBiz {

    ReturnT<String> run(TriggerParam triggerParam);
}
