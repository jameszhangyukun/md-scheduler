package com.md.scheduler.job.core.biz.client;

import com.md.scheduler.job.core.biz.ExecutorBiz;
import com.md.scheduler.job.core.biz.model.IdleBeatParam;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.biz.model.TriggerParam;
import com.md.scheduler.job.core.util.MdJobRemotingUtil;

/**
 * 调度中心触发远程调用
 */
public class ExecutorBizClient implements ExecutorBiz {

    private String addressUrl;
    private String accessToken;
    private int timeout;

    public ExecutorBizClient() {
    }

    public ExecutorBizClient(String addressUrl, String accessToken) {
        this.addressUrl = addressUrl;
        this.accessToken = accessToken;
        if (!this.addressUrl.endsWith("/")) {
            this.addressUrl = this.addressUrl + "/";
        }
    }

    /**
     * 远程调用触发run
     */
    @Override
    public ReturnT<String> run(TriggerParam triggerParam) {
        return MdJobRemotingUtil.postBody(addressUrl + "run", accessToken, timeout, triggerParam, String.class);
    }

    @Override
    public ReturnT<String> idleBeat(IdleBeatParam idleBeatParam) {
        return MdJobRemotingUtil.postBody(addressUrl + "idleBeat", accessToken, timeout, idleBeatParam, String.class);
    }

    @Override
    public ReturnT<String> beat() {
        return MdJobRemotingUtil.postBody(addressUrl + "beat", accessToken, timeout, "", String.class);
    }
}
