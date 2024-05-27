package com.md.scheduler.job.core.biz.client;

import com.md.scheduler.job.core.biz.AdminBiz;
import com.md.scheduler.job.core.biz.model.RegistryParam;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.util.MdJobRemotingUtil;

/**
 * 执行器访问调度中心的客户端
 */
public class AdminBizClient implements AdminBiz {

    private String addressUrl;
    private String accessToken;
    private int timeout = 3;

    public AdminBizClient(String addressUrl, String accessToken) {
        this.addressUrl = addressUrl;
        this.accessToken = accessToken;
        if (!this.addressUrl.endsWith("/")) {
            this.addressUrl = this.addressUrl + "/";
        }
    }

    /**
     * 发送post请求，访问调度中心，将执行器注册到调度中心
     */
    @Override
    public ReturnT<String> registry(RegistryParam registryParam) {
        return MdJobRemotingUtil.postBody(addressUrl + "api/registry", accessToken, timeout, registryParam, String.class);

    }

    @Override
    public ReturnT<String> registryRemove(RegistryParam registryParam) {
        return MdJobRemotingUtil.postBody(addressUrl + "api/registryRemove", accessToken, timeout, registryParam, String.class);
    }
}
