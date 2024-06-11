package com.md.scheduler.job.admin.controller;

import com.md.scheduler.job.admin.core.exception.JobException;
import com.md.scheduler.job.admin.core.model.JobGroup;
import com.md.scheduler.job.admin.core.model.JobInfo;
import com.md.scheduler.job.admin.core.model.JobLog;
import com.md.scheduler.job.admin.core.scheduler.JobScheduler;
import com.md.scheduler.job.admin.core.util.I18nUtil;
import com.md.scheduler.job.admin.dao.JobGroupDao;
import com.md.scheduler.job.admin.dao.JobInfoDao;
import com.md.scheduler.job.admin.dao.JobLogDao;
import com.md.scheduler.job.core.biz.ExecutorBiz;
import com.md.scheduler.job.core.biz.model.LogParam;
import com.md.scheduler.job.core.biz.model.LogResult;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 获取日志信息
 */
@Controller
@RequestMapping("/joblog")
public class JobLogController {
    private static final Logger logger = LoggerFactory.getLogger(JobLogController.class);

    @Resource
    private JobGroupDao xxlJobGroupDao;
    @Resource
    private JobInfoDao xxlJobInfoDao;
    @Resource
    private JobLogDao xxlJobLogDao;


    @RequestMapping
    public String index(HttpServletRequest request, Model model, @RequestParam(required = false, defaultValue = "0") Integer jobId) {
        List<JobGroup> xxlJobGroupDaoAll = xxlJobGroupDao.findAll();
        List<JobGroup> jobGroups = JobInfoController.filterJobGroupByRole(request, xxlJobGroupDaoAll);

        if (jobGroups == null || jobGroups.size() == 0) {
            throw new JobException(I18nUtil.getString("jobgroup_empty"));
        }

        model.addAttribute("JobGroupList", jobGroups);
        if (jobId > 0) {
            JobInfo jobInfo = xxlJobInfoDao.loadById(jobId);
            if (jobInfo == null) {
                throw new RuntimeException(I18nUtil.getString("jobinfo_field_id") + I18nUtil.getString("system_unvalid"));
            }
            model.addAttribute("jobInfo", jobInfo);
            JobInfoController.validPermission(request, jobInfo.getJobGroup());
        }
        return "joblog/joblog.index";
    }
    @RequestMapping("/getJobsByGroup")
    @ResponseBody
    public ReturnT<List<JobInfo>> getJobsByGroup(int jobGroup){
        List<JobInfo> list = xxlJobInfoDao.getJobsByGroup(jobGroup);
        return new ReturnT<List<JobInfo>>(list);
    }


    @RequestMapping("/pageList")
    @ResponseBody
    public Map<String, Object> pageList(HttpServletRequest request,
                                        @RequestParam(required = false, defaultValue = "0") int start,
                                        @RequestParam(required = false, defaultValue = "10") int length,
                                        int jobGroup, int jobId, int logStatus, String filterTime) {
        JobInfoController.validPermission(request, jobGroup);
        Date triggerTimeStart = null;
        Date triggerTimeEnd = null;
        if (filterTime!=null && filterTime.trim().length()>0) {
            String[] temp = filterTime.split(" - ");
            if (temp.length == 2) {
                triggerTimeStart = DateUtil.parseDateTime(temp[0]);
                triggerTimeEnd = DateUtil.parseDateTime(temp[1]);
            }
        }
        List<JobLog> list = xxlJobLogDao.pageList(start, length, jobGroup, jobId, triggerTimeStart, triggerTimeEnd, logStatus);
        int list_count = xxlJobLogDao.pageListCount(start, length, jobGroup, jobId, triggerTimeStart, triggerTimeEnd, logStatus);
        Map<String, Object> maps = new HashMap<String, Object>();
        maps.put("recordsTotal", list_count);
        maps.put("recordsFiltered", list_count);
        maps.put("data", list);
        return maps;
    }


    @RequestMapping("/logDetailPage")
    public String logDetailPage(int id, Model model){
        ReturnT<String> logStatue = ReturnT.SUCCESS;
        JobLog jobLog = xxlJobLogDao.load(id);
        if (jobLog == null) {
            throw new RuntimeException(I18nUtil.getString("joblog_logid_unvalid"));
        }
        model.addAttribute("triggerCode", jobLog.getTriggerCode());
        model.addAttribute("handleCode", jobLog.getHandleCode());
        model.addAttribute("executorAddress", jobLog.getExecutorAddress());
        model.addAttribute("triggerTime", jobLog.getTriggerTime().getTime());
        model.addAttribute("logId", jobLog.getId());
        return "joblog/joblog.detail";
    }



    @RequestMapping("/logDetailCat")
    @ResponseBody
    public ReturnT<LogResult> logDetailCat(String executorAddress, long triggerTime, long logId, int fromLineNum){
        try {
            ExecutorBiz executorBiz = JobScheduler.getExecutorBiz(executorAddress);
            ReturnT<LogResult> logResult = executorBiz.log(new LogParam(triggerTime, logId, fromLineNum));
            if (logResult.getContent()!=null && logResult.getContent().getFromLineNum() > logResult.getContent().getToLineNum()) {
                JobLog jobLog = xxlJobLogDao.load(logId);
                if (jobLog.getHandleCode() > 0) {
                    logResult.getContent().setEnd(true);
                }
            }
            return logResult;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return new ReturnT<LogResult>(ReturnT.FAIL_CODE, e.getMessage());
        }
    }


    //该方法暂且注释掉，用不上
//    @RequestMapping("/logKill")
//    @ResponseBody
//    public ReturnT<String> logKill(int id){
//        // base check
//        XxlJobLog log = xxlJobLogDao.load(id);
//        XxlJobInfo jobInfo = xxlJobInfoDao.loadById(log.getJobId());
//        if (jobInfo==null) {
//            return new ReturnT<String>(500, I18nUtil.getString("jobinfo_glue_jobid_unvalid"));
//        }
//        if (ReturnT.SUCCESS_CODE != log.getTriggerCode()) {
//            return new ReturnT<String>(500, I18nUtil.getString("joblog_kill_log_limit"));
//        }
//        ReturnT<String> runResult = null;
//        try {
//            ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(log.getExecutorAddress());
//            runResult = executorBiz.kill(new KillParam(jobInfo.getId()));
//        } catch (Exception e) {
//            logger.error(e.getMessage(), e);
//            runResult = new ReturnT<String>(500, e.getMessage());
//        }
//        if (ReturnT.SUCCESS_CODE == runResult.getCode()) {
//            log.setHandleCode(ReturnT.FAIL_CODE);
//            log.setHandleMsg( I18nUtil.getString("joblog_kill_log_byman")+":" + (runResult.getMsg()!=null?runResult.getMsg():""));
//            log.setHandleTime(new Date());
//            XxlJobCompleter.updateHandleInfoAndFinish(log);
//            return new ReturnT<String>(runResult.getMsg());
//        } else {
//            return new ReturnT<String>(500, runResult.getMsg());
//        }
//    }

    @RequestMapping("/clearLog")
    @ResponseBody
    public ReturnT<String> clearLog(int jobGroup, int jobId, int type){
        Date clearBeforeTime = null;
        int clearBeforeNum = 0;
        if (type == 1) {
            clearBeforeTime = DateUtil.addMonths(new Date(), -1);
        } else if (type == 2) {
            clearBeforeTime = DateUtil.addMonths(new Date(), -3);
        } else if (type == 3) {
            clearBeforeTime = DateUtil.addMonths(new Date(), -6);
        } else if (type == 4) {
            clearBeforeTime = DateUtil.addYears(new Date(), -1);
        } else if (type == 5) {
            clearBeforeNum = 1000;
        } else if (type == 6) {
            clearBeforeNum = 10000;
        } else if (type == 7) {
            clearBeforeNum = 30000;
        } else if (type == 8) {
            clearBeforeNum = 100000;
        } else if (type == 9) {
            clearBeforeNum = 0;
        } else {
            return new ReturnT<String>(ReturnT.FAIL_CODE, I18nUtil.getString("joblog_clean_type_unvalid"));
        }
        List<Long> logIds = null;
        do {
            logIds = xxlJobLogDao.findClearLogIds(jobGroup, jobId, clearBeforeTime, clearBeforeNum, 1000);
            if (logIds!=null && logIds.size()>0) {
                xxlJobLogDao.clearLog(logIds);
            }
        } while (logIds!=null && logIds.size()>0);
        return ReturnT.SUCCESS;
    }

}