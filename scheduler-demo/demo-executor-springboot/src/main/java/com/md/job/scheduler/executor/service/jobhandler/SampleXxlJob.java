package com.md.job.scheduler.executor.service.jobhandler;

import com.md.scheduler.job.core.handler.annotation.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SampleXxlJob {
    private static Logger logger = LoggerFactory.getLogger(SampleXxlJob.class);


    /**
     * 1、简单任务示例（Bean模式）
     */
    @Job("demoJobHandler")
    public void demoJobHandler() throws Exception {
        for (int i = 0; i < 5; i++) {
            System.out.println("第" + i + "次");
        }
        System.out.println("下一次任务开始了！");
    }
}
