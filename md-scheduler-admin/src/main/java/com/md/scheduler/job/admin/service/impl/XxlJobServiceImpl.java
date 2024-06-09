package com.md.scheduler.job.admin.service.impl;

import com.md.scheduler.job.admin.core.cron.CronExpression;
import com.md.scheduler.job.admin.core.model.JobGroup;
import com.md.scheduler.job.admin.core.model.JobInfo;
import com.md.scheduler.job.admin.core.model.JobLogReport;
import com.md.scheduler.job.admin.core.route.ExecutorRouteStrategyEnum;
import com.md.scheduler.job.admin.core.scheduler.MisfireStrategyEnum;
import com.md.scheduler.job.admin.core.scheduler.ScheduleTypeEnum;
import com.md.scheduler.job.admin.core.thread.JobScheduleHelper;
import com.md.scheduler.job.admin.core.util.I18nUtil;
import com.md.scheduler.job.admin.dao.*;
import com.md.scheduler.job.admin.service.XxlJobService;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.enums.ExecutorBlockStrategyEnum;
import com.md.scheduler.job.core.glue.GlueTypeEnum;
import com.md.scheduler.job.core.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.text.MessageFormat;
import java.util.*;

/**
 * 定时任务的核心逻辑
 */
@Service
public class XxlJobServiceImpl implements XxlJobService {
    private static Logger logger = LoggerFactory.getLogger(XxlJobServiceImpl.class);

    @Resource
    private JobGroupDao xxlJobGroupDao;
    @Resource
    private JobInfoDao xxlJobInfoDao;
    @Resource
    private JobLogDao xxlJobLogDao;
    @Resource
    private JobLogGlueDao xxlJobLogGlueDao;
    @Resource
    private JobLogReportDao xxlJobLogReportDao;

    /**
     * 分页查询定时任务信息
     */
    @Override
    public Map<String, Object> pageList(int start, int length, int jobGroup, int triggerStatus, String jobDesc, String executorHandler, String author) {
        List<JobInfo> list = xxlJobInfoDao.pageList(start, length, jobGroup, triggerStatus, jobDesc, executorHandler, author);
        int list_count = xxlJobInfoDao.pageListCount(start, length, jobGroup, triggerStatus, jobDesc, executorHandler, author);
        Map<String, Object> maps = new HashMap<String, Object>();
        maps.put("recordsTotal", list_count);
        maps.put("recordsFiltered", list_count);
        maps.put("data", list);
        return maps;
    }

    @Override
    public ReturnT<String> add(JobInfo jobInfo) {
        //先查询到该定时任务对应的执行器组
        JobGroup group = xxlJobGroupDao.load(jobInfo.getJobGroup());
        if (group == null) {
            //如果执行器为空，返回失败
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_choose") + I18nUtil.getString("jobinfo_field_jobgroup")));
        }
        //下面是两个判空操作，先判空定时任务描述
        if (jobInfo.getJobDesc() == null || jobInfo.getJobDesc().trim().length() == 0) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_field_jobdesc")));
        }
        //再判空定时任务负责人
        if (jobInfo.getAuthor() == null || jobInfo.getAuthor().trim().length() == 0) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_field_author")));
        }
        //判断前段发送的定时任务是哪种调度类型的
        ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
        if (scheduleTypeEnum == null) {
            //如果为空，则返回失败
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type") + I18nUtil.getString("system_unvalid")));
        }
        //判断是否为cron调度类型
        if (scheduleTypeEnum == ScheduleTypeEnum.CRON) {
            //如果是cron，则判断cron表达式是否正确
            if (jobInfo.getScheduleConf() == null || !CronExpression.isValidExpression(jobInfo.getScheduleConf())) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "Cron" + I18nUtil.getString("system_unvalid"));
            }
        }
        //如果调度类型为按照固定频率
        else if (scheduleTypeEnum == ScheduleTypeEnum.FIX_RATE) {
            if (jobInfo.getScheduleConf() == null) {
                //如果调度规则为空则返回失败
                return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")));
            }
            try {
                //如果规则的值小于1，则返回失败
                int fixSecond = Integer.valueOf(jobInfo.getScheduleConf());
                if (fixSecond < 1) {
                    return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type") + I18nUtil.getString("system_unvalid")));
                }
            } catch (Exception e) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type") + I18nUtil.getString("system_unvalid")));
            }
        }
        //判断用户定义的运行方式是否为空，为空则返回失败
        if (GlueTypeEnum.match(jobInfo.getGlueType()) == null) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_gluetype") + I18nUtil.getString("system_unvalid")));
        }
        //判断jobhandler的名字是否为空，并且运行模式是否为bean模式
        if (GlueTypeEnum.BEAN == GlueTypeEnum.match(jobInfo.getGlueType()) && (jobInfo.getExecutorHandler() == null || jobInfo.getExecutorHandler().trim().length() == 0)) {
            //为空则返回失败
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + "JobHandler"));
        }
        //如果运行模式为在线编码模式，判断源码是否为空
        if (GlueTypeEnum.GLUE_SHELL == GlueTypeEnum.match(jobInfo.getGlueType()) && jobInfo.getGlueSource() != null) {
            //不为空则规整源码
            jobInfo.setGlueSource(jobInfo.getGlueSource().replaceAll("\r", ""));
        }
        //判断路由策略是否为空
        if (ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null) == null) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_executorRouteStrategy") + I18nUtil.getString("system_unvalid")));
        }
        //判断调度失败策略是否为空
        if (MisfireStrategyEnum.match(jobInfo.getMisfireStrategy(), null) == null) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("misfire_strategy") + I18nUtil.getString("system_unvalid")));
        }
        //判断阻塞策略是否为空
        if (ExecutorBlockStrategyEnum.match(jobInfo.getExecutorBlockStrategy(), null) == null) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_executorBlockStrategy") + I18nUtil.getString("system_unvalid")));
        }
        //判断是否有子任务
        if (jobInfo.getChildJobId() != null && jobInfo.getChildJobId().trim().length() > 0) {
            String[] childJobIds = jobInfo.getChildJobId().split(",");
            //如果有则遍历子任务，做相应的判断处理
            for (String childJobIdItem : childJobIds) {
                if (childJobIdItem != null && childJobIdItem.trim().length() > 0 && isNumeric(childJobIdItem)) {
                    JobInfo childJobInfo = xxlJobInfoDao.loadById(Integer.parseInt(childJobIdItem));
                    if (childJobInfo == null) {
                        return new ReturnT<String>(ReturnT.FAIL_CODE,
                                MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId") + "({0})" + I18nUtil.getString("system_not_found")), childJobIdItem));
                    }
                } else {
                    return new ReturnT<String>(ReturnT.FAIL_CODE,
                            MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId") + "({0})" + I18nUtil.getString("system_unvalid")), childJobIdItem));
                }
            }
            //这里是把所有子任务id拼到一块
            StringBuilder temp = new StringBuilder();
            for (String item : childJobIds) {
                temp.append(item).append(",");
            }
            //去掉最后一个句号
            temp = new StringBuilder(temp.substring(0, temp.length() - 1));
            //设置子任务id
            jobInfo.setChildJobId(temp.toString());
        }
        //下面就是定时任务的添加时间，更新时间和glue的更新时间
        jobInfo.setAddTime(new Date());
        jobInfo.setUpdateTime(new Date());
        jobInfo.setGlueUpdatetime(new Date());
        //真正保存定时任务
        xxlJobInfoDao.save(jobInfo);
        if (jobInfo.getId() < 1) {
            //走到这里意味保存失败
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_add") + I18nUtil.getString("system_fail")));
        }
        //走到这里则保存成功
        return new ReturnT<String>(String.valueOf(jobInfo.getId()));
    }

    private boolean isNumeric(String str) {
        try {
            int result = Integer.valueOf(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    public ReturnT<String> update(JobInfo jobInfo) {
        if (jobInfo.getJobDesc() == null || jobInfo.getJobDesc().trim().length() == 0) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_field_jobdesc")));
        }
        if (jobInfo.getAuthor() == null || jobInfo.getAuthor().trim().length() == 0) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_field_author")));
        }
        ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
        if (scheduleTypeEnum == null) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type") + I18nUtil.getString("system_unvalid")));
        }
        if (scheduleTypeEnum == ScheduleTypeEnum.CRON) {
            if (jobInfo.getScheduleConf() == null || !CronExpression.isValidExpression(jobInfo.getScheduleConf())) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "Cron" + I18nUtil.getString("system_unvalid"));
            }
        } else if (scheduleTypeEnum == ScheduleTypeEnum.FIX_RATE /*|| scheduleTypeEnum == ScheduleTypeEnum.FIX_DELAY*/) {
            if (jobInfo.getScheduleConf() == null) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type") + I18nUtil.getString("system_unvalid")));
            }
            try {
                int fixSecond = Integer.valueOf(jobInfo.getScheduleConf());
                if (fixSecond < 1) {
                    return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type") + I18nUtil.getString("system_unvalid")));
                }
            } catch (Exception e) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type") + I18nUtil.getString("system_unvalid")));
            }
        }
        if (ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null) == null) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_executorRouteStrategy") + I18nUtil.getString("system_unvalid")));
        }
        if (MisfireStrategyEnum.match(jobInfo.getMisfireStrategy(), null) == null) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("misfire_strategy") + I18nUtil.getString("system_unvalid")));
        }
        if (ExecutorBlockStrategyEnum.match(jobInfo.getExecutorBlockStrategy(), null) == null) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_executorBlockStrategy") + I18nUtil.getString("system_unvalid")));
        }
        if (jobInfo.getChildJobId() != null && jobInfo.getChildJobId().trim().length() > 0) {
            String[] childJobIds = jobInfo.getChildJobId().split(",");
            for (String childJobIdItem : childJobIds) {
                if (childJobIdItem != null && childJobIdItem.trim().length() > 0 && isNumeric(childJobIdItem)) {
                    JobInfo childJobInfo = xxlJobInfoDao.loadById(Integer.parseInt(childJobIdItem));
                    if (childJobInfo == null) {
                        return new ReturnT<String>(ReturnT.FAIL_CODE,
                                MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId") + "({0})" + I18nUtil.getString("system_not_found")), childJobIdItem));
                    }
                } else {
                    return new ReturnT<String>(ReturnT.FAIL_CODE,
                            MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId") + "({0})" + I18nUtil.getString("system_unvalid")), childJobIdItem));
                }
            }
            String temp = "";
            for (String item : childJobIds) {
                temp += item + ",";
            }
            temp = temp.substring(0, temp.length() - 1);
            jobInfo.setChildJobId(temp);
        }
        JobGroup jobGroup = xxlJobGroupDao.load(jobInfo.getJobGroup());
        if (jobGroup == null) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_jobgroup") + I18nUtil.getString("system_unvalid")));
        }
        //从数据库中查询出旧的定时任务信息
        JobInfo exists_jobInfo = xxlJobInfoDao.loadById(jobInfo.getId());
        if (exists_jobInfo == null) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_id") + I18nUtil.getString("system_not_found")));
        }
        //既然是更新定时任务，下面就要做点不一样的事，先得到定时任务下一次的执行时间
        long nextTriggerTime = exists_jobInfo.getTriggerNextTime();
        //判断调度类型是不是和数据库中存储的相同
        boolean scheduleDataNotChanged = jobInfo.getScheduleType().equals(exists_jobInfo.getScheduleType()) && jobInfo.getScheduleConf().equals(exists_jobInfo.getScheduleConf());
        //如果调度类型不一样，并且定时任务现在处于运行的状态，想想你修改定时任务的cron表达式，它就会在下面这里生效
        if (exists_jobInfo.getTriggerStatus() == 1 && !scheduleDataNotChanged) {
            try {
                //根据新的cron表达式，计算定时任务下一次的执行时间。但这里有一个条件，就是从当前时间加5秒之后的定时任务的
                //执行时间，这么做其实就是在一个新的调度周期中，开始以新的执行时间来调度定时任务
                Date nextValidTime = JobScheduleHelper.generateNextValidTime(jobInfo, new Date(System.currentTimeMillis() + JobScheduleHelper.PRE_READ_MS));
                if (nextValidTime == null) {
                    return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type") + I18nUtil.getString("system_unvalid")));
                }
                //把新的执行时间赋值给上面的nextTriggerTime
                nextTriggerTime = nextValidTime.getTime();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type") + I18nUtil.getString("system_unvalid")));
            }
        }
        //更新旧的定时任务的信息
        exists_jobInfo.setJobGroup(jobInfo.getJobGroup());
        exists_jobInfo.setJobDesc(jobInfo.getJobDesc());
        exists_jobInfo.setAuthor(jobInfo.getAuthor());
        exists_jobInfo.setAlarmEmail(jobInfo.getAlarmEmail());
        exists_jobInfo.setScheduleType(jobInfo.getScheduleType());
        exists_jobInfo.setScheduleConf(jobInfo.getScheduleConf());
        exists_jobInfo.setMisfireStrategy(jobInfo.getMisfireStrategy());
        exists_jobInfo.setExecutorRouteStrategy(jobInfo.getExecutorRouteStrategy());
        exists_jobInfo.setExecutorHandler(jobInfo.getExecutorHandler());
        exists_jobInfo.setExecutorParam(jobInfo.getExecutorParam());
        exists_jobInfo.setExecutorBlockStrategy(jobInfo.getExecutorBlockStrategy());
        exists_jobInfo.setExecutorTimeout(jobInfo.getExecutorTimeout());
        exists_jobInfo.setExecutorFailRetryCount(jobInfo.getExecutorFailRetryCount());
        exists_jobInfo.setChildJobId(jobInfo.getChildJobId());
        exists_jobInfo.setTriggerNextTime(nextTriggerTime);
        exists_jobInfo.setUpdateTime(new Date());
        //跟新定时任务
        xxlJobInfoDao.update(exists_jobInfo);
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> remove(int id) {
        JobInfo xxlJobInfo = xxlJobInfoDao.loadById(id);
        if (xxlJobInfo == null) {
            return ReturnT.SUCCESS;
        }
        xxlJobInfoDao.delete(id);
        xxlJobLogDao.delete(id);
        xxlJobLogGlueDao.deleteByJobId(id);
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> start(int id) {
        JobInfo jobInfo = xxlJobInfoDao.loadById(id);
        ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), ScheduleTypeEnum.NONE);
        if (scheduleTypeEnum == ScheduleTypeEnum.NONE) {
            // 调度类型为空，则不用操作
            return new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString("schedule_type_none_limit_start"));
        }
        long nextTriggerTime = 0;
        try {
            // 新添加的定时任务，在新的调度周期执行，所以需要得到五秒后的时间。
            Date date = JobScheduleHelper.generateNextValidTime(jobInfo, new Date(System.currentTimeMillis() + JobScheduleHelper.PRE_READ_MS));
            if (date == null) {
                return new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString("system_unvalid"));
            }
            // 下一次执行时间赋值
            nextTriggerTime = date.getTime();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString("schedule_type") + I18nUtil.getString("system_unvalid"));
        }
        // 修改定时任务的运行装填，改为运行
        jobInfo.setTriggerStatus(1);
        jobInfo.setTriggerLastTime(0);
        jobInfo.setTriggerNextTime(nextTriggerTime);
        jobInfo.setUpdateTime(new Date());
        xxlJobInfoDao.update(jobInfo);
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> stop(int id) {
        JobInfo jobInfo = xxlJobInfoDao.loadById(id);
        jobInfo.setTriggerStatus(0);
        jobInfo.setTriggerLastTime(0);
        jobInfo.setUpdateTime(new Date());
        jobInfo.setTriggerNextTime(0);
        xxlJobInfoDao.update(jobInfo);
        return ReturnT.SUCCESS;
    }

    @Override
    public Map<String, Object> dashboardInfo() {
        int jobInfoCount = xxlJobInfoDao.findAllCount();
        int jobLogCount = 0;
        int jobLogSuccessCount = 0;
        JobLogReport xxlJobLogReport = xxlJobLogReportDao.queryLogReportTotal();
        if (xxlJobLogReport != null) {
            jobLogCount = xxlJobLogReport.getRunningCount() + xxlJobLogReport.getSucCount() + xxlJobLogReport.getFailCount();
            jobLogSuccessCount = xxlJobLogReport.getSucCount();
        }
        Set<String> executorAddressSet = new HashSet<String>();
        List<JobGroup> groupList = xxlJobGroupDao.findAll();
        if (groupList != null && !groupList.isEmpty()) {
            for (JobGroup group : groupList) {
                if (group.getRegistryList() != null && !group.getRegistryList().isEmpty()) {
                    executorAddressSet.addAll(group.getRegistryList());
                }
            }
        }
        int executorCount = executorAddressSet.size();
        Map<String, Object> dashboardMap = new HashMap<String, Object>();
        dashboardMap.put("jobInfoCount", jobInfoCount);
        dashboardMap.put("jobLogCount", jobLogCount);
        dashboardMap.put("jobLogSuccessCount", jobLogSuccessCount);
        dashboardMap.put("executorCount", executorCount);
        return dashboardMap;
    }

    @Override
    public ReturnT<Map<String, Object>> chartInfo(Date startDate, Date endDate) {
        List<String> triggerDayList = new ArrayList<String>();
        List<Integer> triggerDayCountRunningList = new ArrayList<Integer>();
        List<Integer> triggerDayCountSucList = new ArrayList<Integer>();
        List<Integer> triggerDayCountFailList = new ArrayList<Integer>();
        int triggerCountRunningTotal = 0;
        int triggerCountSucTotal = 0;
        int triggerCountFailTotal = 0;
        List<JobLogReport> logReportList = xxlJobLogReportDao.queryLogReport(startDate, endDate);
        if (logReportList != null && logReportList.size() > 0) {
            for (JobLogReport item : logReportList) {
                String day = DateUtil.formatDate(item.getTriggerDay());
                int triggerDayCountRunning = item.getRunningCount();
                int triggerDayCountSuc = item.getSucCount();
                int triggerDayCountFail = item.getFailCount();
                triggerDayList.add(day);
                triggerDayCountRunningList.add(triggerDayCountRunning);
                triggerDayCountSucList.add(triggerDayCountSuc);
                triggerDayCountFailList.add(triggerDayCountFail);
                triggerCountRunningTotal += triggerDayCountRunning;
                triggerCountSucTotal += triggerDayCountSuc;
                triggerCountFailTotal += triggerDayCountFail;
            }
        } else {
            for (int i = -6; i <= 0; i++) {
                triggerDayList.add(DateUtil.formatDate(DateUtil.addDays(new Date(), i)));
                triggerDayCountRunningList.add(0);
                triggerDayCountSucList.add(0);
                triggerDayCountFailList.add(0);
            }
        }
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("triggerDayList", triggerDayList);
        result.put("triggerDayCountRunningList", triggerDayCountRunningList);
        result.put("triggerDayCountSucList", triggerDayCountSucList);
        result.put("triggerDayCountFailList", triggerDayCountFailList);
        result.put("triggerCountRunningTotal", triggerCountRunningTotal);
        result.put("triggerCountSucTotal", triggerCountSucTotal);
        result.put("triggerCountFailTotal", triggerCountFailTotal);
        return new ReturnT<Map<String, Object>>(result);
    }
}
