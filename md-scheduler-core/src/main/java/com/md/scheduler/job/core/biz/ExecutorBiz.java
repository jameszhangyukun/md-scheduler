package com.md.scheduler.job.core.biz;

import com.md.scheduler.job.core.biz.model.*;

/**
 * 远程调用的客户端接口
 */
public interface ExecutorBiz {

    ReturnT<String> run(TriggerParam triggerParam);

    ReturnT<String> idleBeat(IdleBeatParam idleBeatParam);


    ReturnT<String> beat();

    ReturnT<LogResult> log(LogParam logParam);
}
