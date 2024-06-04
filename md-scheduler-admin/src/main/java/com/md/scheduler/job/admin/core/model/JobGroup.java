package com.md.scheduler.job.admin.core.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * 执行器组
 */

public class JobGroup {

    private int id;
    /***
     * 执行器中配置的项目名称
     */
    private String appname;
    /**
     * 中文名称
     */
    private String title;
    /**
     * 执行器类型，0表示自动注册，1表示手动注册
     */
    private int addressType;
    /**
     * admin端手动填写时，使用ip+port，并且使用逗号分隔
     */
    private String addressList;
    /**
     * 更新时间
     */
    private Date updateTime;
    /**
     * 自动注册时的执行器地址
     */
    private List<String> registryList;

    public List<String> getRegistryList() {
        if (addressList != null && addressList.trim().length() > 0) {
            registryList = new ArrayList<String>(Arrays.asList(addressList.split(",")));
        }
        return registryList;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAppname() {
        return appname;
    }

    public void setAppname(String appname) {
        this.appname = appname;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getAddressType() {
        return addressType;
    }

    public void setAddressType(int addressType) {
        this.addressType = addressType;
    }

    public String getAddressList() {
        return addressList;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public void setAddressList(String addressList) {
        this.addressList = addressList;
    }
}
