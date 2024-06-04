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
}
