package com.md.scheduler.job.core.context;

/**
 * 定时任务上下文
 */
public class JobContext {

    private static InheritableThreadLocal<JobContext> contextHolder = new InheritableThreadLocal<>();

    public static void setContext(JobContext jobContext) {
        contextHolder.set(jobContext);
    }

    public static JobContext getContext() {
        return contextHolder.get();
    }

    public static final int HANDLE_CODE_SUCCESS = 200;

    public static final int HANDLE_CODE_ERROR = 500;

    public static final int HANDLE_CODE_TIMEOUT = 502;
    /**
     * 任务id
     */
    private final long jobId;
    /**
     * 任务参数
     */
    private final String jobParam;
    /**
     * 任务日志名称
     */
    private final String jobLogFileName;
    /**
     * 分片索引
     */
    private final int shardIndex;
    /**
     * 总分片
     */
    private final int shardTotal;
    /**
     * 处理结果
     */
    private int handleCode;
    private String handleMsg;

    public JobContext(long jobId, String jobParam, String jobLogFileName, int shardIndex, int shardTotal) {
        this.jobId = jobId;
        this.jobParam = jobParam;
        this.jobLogFileName = jobLogFileName;
        this.shardIndex = shardIndex;
        this.shardTotal = shardTotal;
        this.handleCode = HANDLE_CODE_SUCCESS;
    }

    public long getJobId() {
        return jobId;
    }

    public String getJobParam() {
        return jobParam;
    }

    public String getJobLogFileName() {
        return jobLogFileName;
    }

    public int getShardIndex() {
        return shardIndex;
    }

    public int getShardTotal() {
        return shardTotal;
    }

    public int getHandleCode() {
        return handleCode;
    }

    public void setHandleCode(int handleCode) {
        this.handleCode = handleCode;
    }

    public String getHandleMsg() {
        return handleMsg;
    }

    public void setHandleMsg(String handleMsg) {
        this.handleMsg = handleMsg;
    }
}
