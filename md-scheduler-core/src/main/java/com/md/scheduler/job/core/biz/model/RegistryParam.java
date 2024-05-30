package com.md.scheduler.job.core.biz.model;

import java.io.Serializable;

/**
 * 执行器注册到调度中心发送的注册请求
 */
public class RegistryParam implements Serializable {
    /**
     * 注册方式
     */
    private String registryGroup;
    /**
     * 执行器的注册名称
     */
    private String registryKey;
    /**
     * 执行器的地址
     */
    private String registryValue;

    public RegistryParam(){}
    public RegistryParam(String registryGroup, String registryKey, String registryValue) {
        this.registryGroup = registryGroup;
        this.registryKey = registryKey;
        this.registryValue = registryValue;
    }

    public String getRegistryGroup() {
        return registryGroup;
    }

    public void setRegistryGroup(String registryGroup) {
        this.registryGroup = registryGroup;
    }

    public String getRegistryKey() {
        return registryKey;
    }

    public void setRegistryKey(String registryKey) {
        this.registryKey = registryKey;
    }

    public String getRegistryValue() {
        return registryValue;
    }

    public void setRegistryValue(String registryValue) {
        this.registryValue = registryValue;
    }

    @Override
    public String toString() {
        return "RegistryParam{" +
                "registryGroup='" + registryGroup + '\'' +
                ", registryKey='" + registryKey + '\'' +
                ", registryValue='" + registryValue + '\'' +
                '}';
    }
}
