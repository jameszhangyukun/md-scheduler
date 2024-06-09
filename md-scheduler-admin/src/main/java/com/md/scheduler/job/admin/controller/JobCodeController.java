package com.md.scheduler.job.admin.controller;

import com.md.scheduler.job.admin.core.model.JobInfo;
import com.md.scheduler.job.admin.core.model.JobLogGlue;
import com.md.scheduler.job.admin.core.util.I18nUtil;
import com.md.scheduler.job.admin.dao.JobInfoDao;
import com.md.scheduler.job.admin.dao.JobLogGlueDao;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.glue.GlueTypeEnum;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.Date;
import java.util.List;


@Controller
@RequestMapping("/jobcode")
public class JobCodeController {

    @Resource
    private JobInfoDao xxlJobInfoDao;
    @Resource
    private JobLogGlueDao xxlJobLogGlueDao;


    @RequestMapping
    public String index(HttpServletRequest request, Model model, int jobId) {
        JobInfo jobInfo = xxlJobInfoDao.loadById(jobId);
        List<JobLogGlue> jobLogGlues = xxlJobLogGlueDao.findByJobId(jobId);
        if (jobInfo == null) {
            throw new RuntimeException(I18nUtil.getString("jobinfo_glue_jobid_unvalid"));
        }
        if (GlueTypeEnum.BEAN == GlueTypeEnum.match(jobInfo.getGlueType())) {
            throw new RuntimeException(I18nUtil.getString("jobinfo_glue_gluetype_unvalid"));
        }
        JobInfoController.validPermission(request, jobInfo.getJobGroup());
        model.addAttribute("GlueTypeEnum", GlueTypeEnum.values());
        model.addAttribute("jobInfo", jobInfo);
        model.addAttribute("jobLogGlues", jobLogGlues);
        return "jobcode/jobcode.index";
    }

    @RequestMapping("/save")
    @ResponseBody
    public ReturnT<String> save(Model model, int id, String glueSource, String glueRemark) {
        if (glueRemark == null) {
            return new ReturnT<String>(500, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_glue_remark")));
        }
        if (glueRemark.length() < 4 || glueRemark.length() > 100) {
            return new ReturnT<String>(500, I18nUtil.getString("jobinfo_glue_remark_limit"));
        }
        JobInfo exists_jobInfo = xxlJobInfoDao.loadById(id);
        if (exists_jobInfo == null) {
            return new ReturnT<String>(500, I18nUtil.getString("jobinfo_glue_jobid_unvalid"));
        }
        exists_jobInfo.setGlueSource(glueSource);
        exists_jobInfo.setGlueRemark(glueRemark);
        exists_jobInfo.setGlueUpdatetime(new Date());
        exists_jobInfo.setUpdateTime(new Date());
        xxlJobInfoDao.update(exists_jobInfo);
        JobLogGlue xxlJobLogGlue = new JobLogGlue();
        xxlJobLogGlue.setJobId(exists_jobInfo.getId());
        xxlJobLogGlue.setGlueType(exists_jobInfo.getGlueType());
        xxlJobLogGlue.setGlueSource(glueSource);
        xxlJobLogGlue.setGlueRemark(glueRemark);
        xxlJobLogGlue.setAddTime(new Date());
        xxlJobLogGlue.setUpdateTime(new Date());
        xxlJobLogGlueDao.save(xxlJobLogGlue);
        xxlJobLogGlueDao.removeOld(exists_jobInfo.getId(), 30);
        return ReturnT.SUCCESS;
    }

}
