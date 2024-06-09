package com.md.scheduler.job.admin.core.conf;

import com.md.scheduler.job.admin.core.scheduler.JobScheduler;
import com.md.scheduler.job.admin.dao.*;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.Arrays;

/**
 * admin控制台配置
 */
@Component
public class JobAdminConfig implements InitializingBean, DisposableBean {

    private static JobAdminConfig jobAdminConfig;

    public static JobAdminConfig getAdminConfig() {
        return jobAdminConfig;
    }

    private JobScheduler xxlJobScheduler;

    @Value("${xxl.job.i18n}")
    private String i18n;

    @Value("${xxl.job.accessToken}")
    private String accessToken;

    @Value("${spring.mail.from}")
    private String emailFrom;

    //快线程池的最大线程数
    @Value("${xxl.job.triggerpool.fast.max}")
    private int triggerPoolFastMax;

    //慢线程池的最大线程数
    @Value("${xxl.job.triggerpool.slow.max}")
    private int triggerPoolSlowMax;

    //该属性是日志保留时间的意思
    @Value("${xxl.job.logretentiondays}")
    private int logretentiondays;


    @Resource
    private JobLogDao xxlJobLogDao;
    @Resource
    private JobInfoDao xxlJobInfoDao;
    @Resource
    private JobRegistryDao xxlJobRegistryDao;
    @Resource
    private JobGroupDao xxlJobGroupDao;
    @Resource
    private JobLogReportDao xxlJobLogReportDao;
    @Resource
    private JavaMailSender mailSender;
    @Resource
    private DataSource dataSource;

    @Override
    public void destroy() throws Exception {
        xxlJobScheduler.destroy();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        jobAdminConfig = this;
        xxlJobScheduler = new JobScheduler();
        xxlJobScheduler.init();
    }

    public String getI18n() {
        if (!Arrays.asList("zh_CN", "zh_TC", "en").contains(i18n)) {
            return "zh_CN";
        }
        return i18n;
    }

    public String getAccessToken() {
        return accessToken;
    }


    public String getEmailFrom() {
        return emailFrom;
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


    public int getLogretentiondays() {
        if (logretentiondays < 7) {
            return -1;
        }
        return logretentiondays;
    }

    public JobLogDao getXxlJobLogDao() {
        return xxlJobLogDao;
    }

    public JobInfoDao getXxlJobInfoDao() {
        return xxlJobInfoDao;
    }

    public JobRegistryDao getXxlJobRegistryDao() {
        return xxlJobRegistryDao;
    }

    public JobGroupDao getXxlJobGroupDao() {
        return xxlJobGroupDao;
    }

    public JobLogReportDao getXxlJobLogReportDao() {
        return xxlJobLogReportDao;
    }

    public JavaMailSender getMailSender() {
        return mailSender;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

}
