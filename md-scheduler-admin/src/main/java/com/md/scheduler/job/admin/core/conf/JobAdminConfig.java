package com.md.scheduler.job.admin.core.conf;

import com.md.scheduler.job.admin.dao.JobRegistryDao;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Arrays;

/**
 * admin控制台配置
 */
@Component
public class JobAdminConfig implements InitializingBean, DisposableBean {

    private static JobAdminConfig jobAdminConfig;

    public static JobAdminConfig  getAdminConfig() {
        return jobAdminConfig;
    }

    //快线程池的最大线程数
    @Value("${xxl.job.triggerpool.fast.max}")
    private int triggerPoolFastMax;

    //慢线程池的最大线程数
    @Value("${xxl.job.triggerpool.slow.max}")
    private int triggerPoolSlowMax;
    @Value("${xxl.job.i18n}")
    private String i18n;
    @Resource
    private JobRegistryDao jobRegistryDao;

    @Override
    public void destroy() throws Exception {
        jobAdminConfig = this;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

    }
    public String getI18n() {
        if (!Arrays.asList("zh_CN", "zh_TC", "en").contains(i18n)) {
            return "zh_CN";
        }
        return i18n;
    }
    public JobRegistryDao getXxlJobRegistryDao() {
        return jobRegistryDao;
    }

    public int getTriggerPoolFastMax() {
        if (triggerPoolFastMax < 200) {
            return 200;
        }
        return triggerPoolFastMax;
    }

    public int getTriggerPoolSlowMax() {
        if (triggerPoolSlowMax < 100) {
            return 100;
        }
        return triggerPoolSlowMax;
    }
}
