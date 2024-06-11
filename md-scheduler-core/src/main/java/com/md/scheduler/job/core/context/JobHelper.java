package com.md.scheduler.job.core.context;

import com.md.scheduler.job.core.log.XxlJobFileAppender;
import com.md.scheduler.job.core.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

/**
 * 日志处理
 */
public class JobHelper {
    private static Logger logger = LoggerFactory.getLogger("xxl-job logger");

    /**
     * 获取定时任务id
     */
    public static long getJobId() {
        JobContext context = JobContext.getContext();
        if (context == null) {
            return -1;
        }
        return context.getJobId();
    }

    //获取定时任务的执行参数
    public static String getJobParam() {
        JobContext jobContext = JobContext.getContext();
        if (jobContext == null) {
            return null;
        }

        return jobContext.getJobParam();
    }


    //获取定时任务的日志记录的文件名称
    public static String getJobLogFileName() {
        JobContext jobContext = JobContext.getContext();
        if (jobContext == null) {
            return null;
        }
        return jobContext.getJobLogFileName();
    }


    //获取分片索引，这里还用不到
    public static int getShardIndex() {
        JobContext jobContext = JobContext.getContext();
        if (jobContext == null) {
            return -1;
        }

        return jobContext.getShardIndex();
    }

    //获取分片总数，这里也用不到
    public static int getShardTotal() {
        JobContext jobContext = JobContext.getContext();
        if (jobContext == null) {
            return -1;
        }

        return jobContext.getShardTotal();
    }

    public static boolean log(String appendLogPattern, Object... appendLogArguments) {
        //该方法的作用是用来格式化要记录的日志信息
        FormattingTuple ft = MessageFormatter.arrayFormat(appendLogPattern, appendLogArguments);
        String appendLog = ft.getMessage();
        //从栈帧中获得方法的调用信息
        StackTraceElement callInfo = new Throwable().getStackTrace()[1];
        //在这里开始存储日志，但这里实际上只是个入口方法，真正的操作还是会进一步调用XxlJobFileAppender类的方法来完成的
        return logDetail(callInfo, appendLog);
    }

    public static boolean log(Throwable e) {
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        String appendLog = stringWriter.toString();
        StackTraceElement callInfo = new Throwable().getStackTrace()[1];
        return logDetail(callInfo, appendLog);
    }

    private static boolean logDetail(StackTraceElement callInfo, String appendLog) {
        //从当前线程中获得定时任务上下文对象
        JobContext xxlJobContext = JobContext.getContext();
        if (xxlJobContext == null) {
            return false;
        }
        StringBuffer stringBuffer = new StringBuffer();
        //在这里把方法调用的详细信息拼接一下
        stringBuffer.append(DateUtil.formatDateTime(new Date())).append(" ")
                .append("[" + callInfo.getClassName() + "#" + callInfo.getMethodName() + "]").append("-")
                .append("[" + callInfo.getLineNumber() + "]").append("-")
                .append("[" + Thread.currentThread().getName() + "]").append(" ")
                .append(appendLog != null ? appendLog : "");
        //转换成字符串
        String formatAppendLog = stringBuffer.toString();
        //获取定时任务对应的日志存储路径
        String logFileName = xxlJobContext.getJobLogFileName();
        if (logFileName != null && logFileName.trim().length() > 0) {
            //真正存储日志的方法，在这里就把日志存储到本地文件了
            XxlJobFileAppender.appendLog(logFileName, formatAppendLog);
            return true;
        } else {
            logger.info(">>>>>>>>>>> {}", formatAppendLog);
            return false;
        }
    }

    public static boolean handleSuccess() {
        return handleResult(JobContext.HANDLE_CODE_SUCCESS, null);
    }


    public static boolean handleSuccess(String handleMsg) {
        return handleResult(JobContext.HANDLE_CODE_SUCCESS, handleMsg);
    }


    public static boolean handleFail() {
        return handleResult(JobContext.HANDLE_CODE_FAIL, null);
    }


    public static boolean handleFail(String handleMsg) {
        return handleResult(JobContext.HANDLE_CODE_FAIL, handleMsg);
    }


    public static boolean handleTimeout() {
        return handleResult(JobContext.HANDLE_CODE_TIMEOUT, null);
    }


    public static boolean handleTimeout(String handleMsg) {
        return handleResult(JobContext.HANDLE_CODE_TIMEOUT, handleMsg);
    }


    public static boolean handleResult(int handleCode, String handleMsg) {
        JobContext xxlJobContext = JobContext.getContext();
        if (xxlJobContext == null) {
            return false;
        }
        xxlJobContext.setHandleCode(handleCode);
        if (handleMsg != null) {
            xxlJobContext.setHandleMsg(handleMsg);
        }
        return true;
    }
}

