package com.md.scheduler.job.admin.controller;

import com.md.scheduler.job.admin.controller.annotation.PermissionLimit;
import com.md.scheduler.job.admin.core.conf.JobAdminConfig;
import com.md.scheduler.job.core.biz.AdminBiz;
import com.md.scheduler.job.core.biz.model.RegistryParam;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.util.GsonTool;
import com.md.scheduler.job.core.util.MdJobRemotingUtil;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;


@Controller
@RequestMapping("/api")
public class JobApiController {

    @Resource
    private AdminBiz adminBiz;


    @RequestMapping("/{uri}")
    @ResponseBody
    @PermissionLimit(limit = false)
    public ReturnT<String> api(HttpServletRequest request, @PathVariable("uri") String uri, @RequestBody(required = false) String data) {
        //判断是不是post请求
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, HttpMethod not support.");
        }
        //对路径做判空处理
        if (uri == null || uri.trim().length() == 0) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping empty.");
        }
        //判断执行器配置的token和调度中心的是否相等
        if (JobAdminConfig.getAdminConfig().getAccessToken() != null
                && JobAdminConfig.getAdminConfig().getAccessToken().trim().length() > 0
                && !JobAdminConfig.getAdminConfig().getAccessToken().equals(request.getHeader(MdJobRemotingUtil.MD_JOB_ACCESS_TOKEN))) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "The access token is wrong.");
        }
        //判断是不是注册操作
        else if ("registry".equals(uri)) {
            RegistryParam registryParam = GsonTool.fromJson(data, RegistryParam.class);
            //执行注册任务
            return adminBiz.registry(registryParam);
            //判断是不是从调度中心移除执行器的操作
        } else if ("registryRemove".equals(uri)) {
            RegistryParam registryParam = GsonTool.fromJson(data, RegistryParam.class);
            //执行移除任务
            return adminBiz.registryRemove(registryParam);
        } else {
            //都不匹配则返回失败
            return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping(" + uri + ") not found.");
        }

    }

}
