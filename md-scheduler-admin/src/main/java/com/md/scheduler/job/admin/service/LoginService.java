package com.md.scheduler.job.admin.service;

import com.md.scheduler.job.admin.core.model.JobUser;
import com.md.scheduler.job.admin.core.util.CookieUtil;
import com.md.scheduler.job.admin.core.util.I18nUtil;
import com.md.scheduler.job.admin.core.util.JacksonUtil;
import com.md.scheduler.job.admin.dao.JobUserDao;
import com.md.scheduler.job.core.biz.model.ReturnT;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.DigestUtils;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.math.BigInteger;

/**
 * 登录
 */
@Configuration
public class LoginService {
    /**
     * 用户登录的身份
     */
    public static final String LOGIN_IDENTITY_KEY = "XXL_JOB_LOGIN_IDENTITY";
    @Resource
    private JobUserDao jobUserDao;

    /**
     * 制作用户token
     */
    private String makeToken(JobUser jobUser) {
        String tokenJson = JacksonUtil.writeValueAsString(jobUser);
        return new BigInteger(tokenJson.getBytes()).toString(16);
    }

    /**
     * 解析token
     */
    private JobUser parseToken(String tokenHex) {
        if (tokenHex == null) {
            return null;
        }
        String tokenJson = new String(new BigInteger(tokenHex, 16).toByteArray());
        return JacksonUtil.readValue(tokenJson, JobUser.class);
    }

    /**
     * 登录
     */
    public ReturnT<String> login(HttpServletRequest request, HttpServletResponse response, String username, String password, boolean ifRemember) {
        if (username == null || username.trim().length() == 0 || password == null || password.trim().length() == 0) {
            return new ReturnT<String>(500, I18nUtil.getString("login_param_empty"));
        }
        JobUser xxlJobUser = jobUserDao.loadByUserName(username);
        if (xxlJobUser == null) {
            return new ReturnT<String>(500, I18nUtil.getString("login_param_unvalid"));
        }
        String passwordMd5 = DigestUtils.md5DigestAsHex(password.getBytes());
        if (!passwordMd5.equals(xxlJobUser.getPassword())) {
            return new ReturnT<String>(500, I18nUtil.getString("login_param_unvalid"));
        }
        String loginToken = makeToken(xxlJobUser);
        CookieUtil.set(response, LOGIN_IDENTITY_KEY, loginToken, ifRemember);
        return ReturnT.SUCCESS;
    }


    public JobUser ifLogin(HttpServletRequest request, HttpServletResponse response) {
        String cookieToken = CookieUtil.getValue(request, LOGIN_IDENTITY_KEY);
        if (cookieToken != null) {
            JobUser cookieUser = null;
            try {
                cookieUser = parseToken(cookieToken);
            } catch (Exception e) {
                logout(request, response);
            }
            if (cookieUser != null) {
                JobUser dbUser = jobUserDao.loadByUserName(cookieUser.getUsername());
                if (dbUser != null) {
                    if (cookieUser.getPassword().equals(dbUser.getPassword())) {
                        return dbUser;
                    }
                }
            }
        }
        return null;
    }

    public ReturnT<String> logout(HttpServletRequest request, HttpServletResponse response) {
        CookieUtil.remove(request, response, LOGIN_IDENTITY_KEY);
        return ReturnT.SUCCESS;
    }
}
