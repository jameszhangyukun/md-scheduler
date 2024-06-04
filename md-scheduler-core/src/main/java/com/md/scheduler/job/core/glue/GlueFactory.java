package com.md.scheduler.job.core.glue;

import com.md.scheduler.job.core.handler.IJobHandler;
import groovy.lang.GroovyClassLoader;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 运行模式工厂
 */
public class GlueFactory {

    private static GlueFactory glueFactory = new GlueFactory();

    public static GlueFactory getInstance() {
        return glueFactory;
    }

    public static void refreshInstance(int type) {
        if (type == 0) {
            glueFactory = new GlueFactory();
        } else if (type == 1) {
            glueFactory = new GlueFactory();
        }
    }

    private GroovyClassLoader groovyClassLoader = new GroovyClassLoader();

    private ConcurrentHashMap<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();

    public IJobHandler loadNewInstance(String className) throws Exception {
        if (className != null && className.trim().length() > 0) {
            Class<?> clazz = groovyClassLoader.parseClass(className);
            if (clazz != null) {
                Object instance = clazz.newInstance();
                if (instance != null) {
                    if (instance instanceof IJobHandler) {
                        this.injectService(instance);
                        return (IJobHandler) instance;
                    } else {
                        throw new IllegalArgumentException(">>>>>>>>>>> glue, loadNewInstance error, "
                                + "cannot convert from instance[" + instance.getClass() + "] to IJobHandler");
                    }
                }
            }
        }
        throw new IllegalArgumentException(">>>>>>>>>>> glue, loadNewInstance error, instance is null");

    }


    private Class<?> getCodeSourceClass(String codeSource) {
        try {
            byte[] md5 = MessageDigest.getInstance("MD5").digest(codeSource.getBytes());
            String md5Str = new BigInteger(1, md5).toString(16);
            Class<?> aClass = CLASS_CACHE.get(md5Str);
            if (aClass == null) {
                aClass = groovyClassLoader.parseClass(codeSource);
                CLASS_CACHE.put(md5Str, aClass);
            }
            return aClass;
        } catch (Exception e) {
            return groovyClassLoader.parseClass(codeSource);
        }
    }

    public void injectService(Object instance) {

    }
}
