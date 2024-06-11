package com.md.scheduler.job.core.executor;

import com.md.scheduler.job.core.biz.AdminBiz;
import com.md.scheduler.job.core.biz.client.AdminBizClient;
import com.md.scheduler.job.core.handler.IJobHandler;
import com.md.scheduler.job.core.handler.annotation.Job;
import com.md.scheduler.job.core.handler.impl.MethodJobHandler;
import com.md.scheduler.job.core.log.XxlJobFileAppender;
import com.md.scheduler.job.core.server.EmbedServer;
import com.md.scheduler.job.core.thread.JobThread;
import com.md.scheduler.job.core.util.IpUtil;
import com.md.scheduler.job.core.util.NetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 执行器启动类，从子类开始执行，会调用父类的start方法，真正启动执行器组件
 */
public class JobExecutor {
    private static final Logger logger = LoggerFactory.getLogger(JobExecutor.class);
    /**
     * 注册中心地址
     */
    private String adminAddresses;
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
    private EmbedServer embedServer = null;

    private final static ConcurrentHashMap<String, IJobHandler> jobHandlerRepository = new ConcurrentHashMap<>();
    private static ConcurrentMap<Integer, JobThread> jobThreadRepository = new ConcurrentHashMap<Integer, JobThread>();
    public void setAdminAddresses(String adminAddresses) {
        this.adminAddresses = adminAddresses;
    }
    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }
    public void setAppname(String appname) {
        this.appname = appname;
    }
    public void setAddress(String address) {
        this.address = address;
    }
    public void setIp(String ip) {
        this.ip = ip;
    }
    public void setPort(int port) {
        this.port = port;
    }
    public void setLogPath(String logPath) {
        this.logPath = logPath;
    }
    public void setLogRetentionDays(int logRetentionDays) {
        this.logRetentionDays = logRetentionDays;
    }

    /**
     * 执行器启动
     *
     * @throws Exception
     */
    public void start() throws Exception {
        //初始化日记收集组件，并且把用户设置的存储日记的路径设置到该组件中
        XxlJobFileAppender.initLogPath(logPath);
        initAdminBizList(adminAddresses, accessToken);
        initEmbedServer(adminAddresses, ip, port, appname, accessToken);
    }

    public void destroy() {
        stopEmbedServer();
        if (jobThreadRepository.size() > 0) {
            for (Map.Entry<Integer, JobThread> item : jobThreadRepository.entrySet()) {
                JobThread oldJobThread = removeJobThread(item.getKey(), "web container destroy and kill the job.");
                if (oldJobThread != null) {
                    try {
                        oldJobThread.join();
                    } catch (InterruptedException e) {
                        logger.error(">>>>>>>>>>> job, JobThread destroy(join) error, jobId:{}", item.getKey(), e);
                    }
                }
            }
            jobHandlerRepository.clear();
        }
        jobHandlerRepository.clear();
    }

    /**
     * 启动内嵌的Netty服务器
     *
     * @param address
     * @param ip
     * @param port
     * @param appname
     * @param accessToken
     */
    private void initEmbedServer(String address, String ip, Integer port, String appname, String accessToken) {
        port = port > 0 ? port : NetUtil.findAvailablePort(port);
        ip = (ip != null && ip.trim().length() > 0) ? ip : IpUtil.getIp();
        if (address == null || address.trim().length() == 0) {
            // 没有设置地址，将ip和port凭借
            String ipPortAddress = IpUtil.getIpPort(ip, port);
            address = "http://{ip_port}".replace("{ip_port}", ipPortAddress);
        }
        if (accessToken == null || accessToken.trim().length() == 0) {
            logger.warn(">>>>>>>>>>>job accessToken is empty. To ensure system security, please set the accessToken.");

        }
        //创建执行器端的Netty服务器
        embedServer = new EmbedServer();
        //启动服务器，在启动的过程中，会把执行器注册到调度中心
        embedServer.start(address, port, appname, accessToken);
    }

    private void stopEmbedServer() {
        if (embedServer != null) {
            try {
                embedServer.stop();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
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

    public static IJobHandler loadJobHandler(String name) {
        return jobHandlerRepository.get(name);
    }


    public static IJobHandler registerJobHandler(String name, IJobHandler jobHandler) {
        logger.info(">>>>>>>>>>> job register jobhandler success, name:{}, jobHandler:{}", name, jobHandler);
        return jobHandlerRepository.put(name, jobHandler);
    }

    /**
     * 将用户定义的bean中的每个定时任务方法都注册到JobHandler中
     *
     * @param job
     * @param bean
     * @param executeMethod
     */
    protected void registerJobHandler(Job job, Object bean, Method executeMethod) {
        if (job == null) {
            return;
        }
        String name = job.value();
        Class<?> clazz = bean.getClass();
        String methodName = executeMethod.getName();

        if (name == null || name.trim().length() == 0) {
            throw new RuntimeException("job method-job-handler name invalid, for[" + clazz + "#" + methodName + "] .");
        }
        if (loadJobHandler(name) != null) {
            throw new RuntimeException("job job-handler[" + name + "] naming conflicts.");
        }

        executeMethod.setAccessible(true);
        Method initMethod = null;
        Method destroyMethod = null;
        if (job.init().trim().length() > 0) {
            try {
                initMethod = clazz.getDeclaredMethod(job.init());
                initMethod.setAccessible(true);
            } catch (Exception e) {
                throw new RuntimeException("job method-jobhandler destroyMethod invalid, for[" + clazz + "#" + methodName + "] .");
            }
        }
        if (job.destroy().trim().length() > 0) {
            try {
                destroyMethod = clazz.getDeclaredMethod(job.destroy());
                destroyMethod.setAccessible(true);
            } catch (Exception e) {
                throw new RuntimeException("job method-jobhandler destroyMethod invalid, for[" + clazz + "#" + methodName + "] .");
            }
        }
        registerJobHandler(name, new MethodJobHandler(bean, executeMethod, initMethod, destroyMethod));
    }

    /**
     * 把定时任务相应的JobThread缓存到Map中
     */
    public static JobThread registerJobThread(int jobId, IJobHandler jobHandler, String removeOldReason) {
        JobThread jobThread = new JobThread(jobId, jobHandler);
        jobThread.start();
        logger.info(">>>>>>>>>>> job register JobThread success, jobId:{}, handler:{}", jobId, jobHandler);
        JobThread oldJobThread = jobThreadRepository.put(jobId, jobThread);
        if (oldJobThread != null) {
            oldJobThread.toStop(removeOldReason);
            oldJobThread.interrupt();
        }
        return jobThread;
    }

    public static JobThread removeJobThread(int threadId, String removeOldReason) {
        JobThread oldJobThread = jobThreadRepository.remove(threadId);
        if (oldJobThread != null) {
            oldJobThread.toStop(removeOldReason);
            oldJobThread.interrupt();
            return oldJobThread;
        }
        return null;
    }

    public static JobThread loadJobThread(int jobId) {
        return jobThreadRepository.get(jobId);
    }

    public static List<AdminBiz> getAdminBizList() {
        return adminBizList;
    }


    public String getAdminAddress() {
        return adminAddresses;
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
