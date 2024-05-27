package com.md.scheduler.job.core.server;

import com.md.scheduler.job.core.biz.ExecutorBiz;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 执行器内嵌的netty服务器
 */
public class EmbedServer {

    private static final Logger logger = LoggerFactory.getLogger(EmbedServer.class);

    /**
     * 执行器接口，在start方法中初始化
     */
    private ExecutorBiz executorBiz;
    /**
     * 启动netty服务器的线程
     */
    private Thread thread;



}
