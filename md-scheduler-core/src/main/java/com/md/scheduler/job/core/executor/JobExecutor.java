package com.md.scheduler.job.core.executor;

import com.md.scheduler.job.core.biz.AdminBiz;
import com.md.scheduler.job.core.biz.client.AdminBizClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 执行器启动类，从子类开始执行，会调用父类的start方法，真正启动执行器组件
 */
public class JobExecutor {
    private static final Logger logger = LoggerFactory.getLogger(JobExecutor.class);
    /**
     * 注册中心地址
     */
    private String adminAddress;
    /**
     * token
     */
    private String accessToken;
    /**
     * 执行器注册到调度中心的名称
     */
    private String appname;
    /**
     * 地址，ip+port
     */
    private String address;
    private String ip;
    private int port;
    private String logPath;
    private int logRetentionDays;

    //该成员变量是用来存放AdminBizClient对象的，而该对象是用来向调度中心发送注册信息的
    private static List<AdminBiz> adminBizList;

    //内嵌的服务器对象
//    private EmbedServer embedServer = null;

    /**
     * 执行器启动
     *
     * @throws Exception
     */
    public void start() throws Exception {
        // TODO 注册

        // TODO 启动内部Netty Server
    }

    private void initAdminBizList(String adminAddresses, String accessToken) throws Exception {
        if (adminAddresses != null && adminAddresses.trim().length() > 0) {
            for (String address : adminAddresses.trim().split(",")) {
                if (address != null && address.trim().length() > 0) {
                    //根据服务器地址和令牌创建一个客户端
                    AdminBiz adminBiz = new AdminBizClient(address.trim(), accessToken);
                    //如果AdminBizClient对象为空，就初始化集合对象
                    if (adminBizList == null) {
                        adminBizList = new ArrayList<AdminBiz>();
                    }
                    //把创建好的客户端添加到集合中
                    adminBizList.add(adminBiz);
                }
            }
        }
    }


    public static List<AdminBiz> getAdminBizList(){
        return adminBizList;
    }



    public String getAdminAddress() {
        return adminAddress;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getAppname() {
        return appname;
    }

    public String getAddress() {
        return address;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public String getLogPath() {
        return logPath;
    }

    public int getLogRetentionDays() {
        return logRetentionDays;
    }
}
