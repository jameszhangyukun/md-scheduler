package com.md.scheduler.job.core.handler.impl;

import com.md.scheduler.job.core.handler.IJobHandler;

import java.lang.reflect.Method;

/**
 * 反射调用执行方法逻辑
 */
public class MethodJobHandler extends IJobHandler {
    /**
     * 目标类对象，用户定义的IOC容器中的bean
     */
    private final Object target;
    /**
     * 执行定时任务的方法
     */
    private final Method method;

    private Method initMethod;

    private Method destroyMethod;

    public MethodJobHandler(Object target, Method method, Method initMethod, Method destroyMethod) {
        this.target = target;
        this.method = method;
        this.initMethod = initMethod;
        this.destroyMethod = destroyMethod;
    }

    @Override
    public void execute() throws Exception {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length > 0) {
            method.invoke(target, new Object[parameterTypes.length]);
        } else {
            method.invoke(target);
        }
    }

    @Override
    public void init() throws Exception {
        if (initMethod != null) {
            initMethod.invoke(target);
        }
    }

    //反射调用目标对象的destroy方法
    @Override
    public void destroy() throws Exception {
        if (destroyMethod != null) {
            destroyMethod.invoke(target);
        }
    }

    @Override
    public String toString() {
        return super.toString() + "[" + target.getClass() + "#" + method.getName() + "]";
    }
}
