package com.md.scheduler.job.core.executor.impl;

import com.md.scheduler.job.core.executor.JobExecutor;
import com.md.scheduler.job.core.handler.annotation.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * 将JobConfig配置类，当做bean对象注入到IOC容器中
 */
public class JobSpringExecutor extends JobExecutor implements ApplicationContextAware, SmartInitializingSingleton, DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(JobSpringExecutor.class);

    @Override
    public void destroy() {
        super.destroy();
    }

    @Override
    public void afterSingletonsInstantiated() {
        initJobHandlerMethodRepository(applicationContext);

        try {
            super.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initJobHandlerMethodRepository(ApplicationContext applicationContext) {
        if (applicationContext == null) {
            return;
        }
        // 获取IOC容器中所有初始好的bean名称，第一个boolean表示 是否允许非单例，第二个表示是否允许延迟初始化
        String[] beanNamesForType = applicationContext.getBeanNamesForType(Object.class, false, true);
        for (String beanDefinitionName : beanNamesForType) {
            Object bean = applicationContext.getBean(beanDefinitionName);
            Map<Method, Job> annotatedMethods = null;
            try {
                annotatedMethods =
                        MethodIntrospector.selectMethods(bean.getClass(), (MethodIntrospector.MetadataLookup<Job>) method -> AnnotatedElementUtils.findMergedAnnotation(method, Job.class));
            } catch (Throwable ex) {
                logger.error("job method-jobhandler resolve error for bean[" + beanDefinitionName + "].", ex);
            }
            if (annotatedMethods == null || annotatedMethods.isEmpty()) {
                continue;
            }
            for (Map.Entry<Method, Job> methodJobEntry : annotatedMethods.entrySet()) {
                Method key = methodJobEntry.getKey();
                Job value = methodJobEntry.getValue();
                registerJobHandler(value, bean, key);
            }
        }

    }

    private static ApplicationContext applicationContext;


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        JobSpringExecutor.applicationContext = applicationContext;
    }

    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }
}
