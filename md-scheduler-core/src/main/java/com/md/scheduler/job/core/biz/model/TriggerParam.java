package com.md.scheduler.job.core.biz.model;

import java.io.PrintWriter;
import java.io.Serializable;

/**
 * 封装触发器信息的实体类，调度中心执行远程调用下发的请求
 */
public class TriggerParam implements Serializable {
    /**
     * 定时任务id
     */
    private int jobId;

    /**
     * JobHandler的名称
     */
    private String executorHandler;
    /**
     * 定时任务执行的参数
     */
    private String executorParams;
    /**
     * 阻塞策略
     */
    private String executorBlockStrategy;
    /**
     * 超时时间
     */
    private int executorTimeout;
    /**
     * 日志id
     */
    private long logId;
    /**
     * 日志时间
     */
    private long logDateTime;
    /**
     * 运行模式
     */
    private String glueType;
    /**
     * 代码文本
     */
    private String glueSource;
    /**
     * glue更新时间
     */
    private long glueUpdateTime;
    /**
     * 分片索引
     */
    private int broadcastIndex;
    /**
     * 分片总数
     */
    private int broadcastTotal;

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public String getExecutorHandler() {
        return executorHandler;
    }

    public void setExecutorHandler(String executorHandler) {
        this.executorHandler = executorHandler;
    }

    public String getExecutorParams() {
        return executorParams;
    }

    public void setExecutorParams(String executorParams) {
        this.executorParams = executorParams;
    }

    public String getExecutorBlockStrategy() {
        return executorBlockStrategy;
    }

    public void setExecutorBlockStrategy(String executorBlockStrategy) {
        this.executorBlockStrategy = executorBlockStrategy;
    }

    public int getExecutorTimeout() {
        return executorTimeout;
    }

    public void setExecutorTimeout(int executorTimeout) {
        this.executorTimeout = executorTimeout;
    }

    public long getLogId() {
        return logId;
    }

    public void setLogId(long logId) {
        this.logId = logId;
    }

    public long getLogDateTime() {
        return logDateTime;
    }

    public void setLogDateTime(long logDateTime) {
        this.logDateTime = logDateTime;
    }

    public String getGlueType() {
        return glueType;
    }

    public void setGlueType(String glueType) {
        this.glueType = glueType;
    }

    public String getGlueSource() {
        return glueSource;
    }

    public void setGlueSource(String glueSource) {
        this.glueSource = glueSource;
    }

    public long getGlueUpdateTime() {
        return glueUpdateTime;
    }

    public void setGlueUpdateTime(long glueUpdateTime) {
        this.glueUpdateTime = glueUpdateTime;
    }

    public int getBroadcastIndex() {
        return broadcastIndex;
    }

    public void setBroadcastIndex(int broadcastIndex) {
        this.broadcastIndex = broadcastIndex;
    }

    public int getBroadcastTotal() {
        return broadcastTotal;
    }

    public void setBroadcastTotal(int broadcastTotal) {
        this.broadcastTotal = broadcastTotal;
    }

    @Override
    public String toString() {
        return "TriggerParam{" +
                "jobId=" + jobId +
                ", executorHandler='" + executorHandler + '\'' +
                ", executorParams='" + executorParams + '\'' +
                ", executorBlockStrategy='" + executorBlockStrategy + '\'' +
                ", executorTimeout=" + executorTimeout +
                ", logId=" + logId +
                ", logDateTime=" + logDateTime +
                ", glueType='" + glueType + '\'' +
                ", glueSource='" + glueSource + '\'' +
                ", glueUpdateTime=" + glueUpdateTime +
                ", broadcastIndex=" + broadcastIndex +
                ", broadcastTotal=" + broadcastTotal +
                '}';
    }
}
