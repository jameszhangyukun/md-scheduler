package com.md.scheduler.job.core.util;

import com.md.scheduler.job.core.biz.model.ReturnT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * 远程调用工具类
 */
public class MdJobRemotingUtil {
    private static final Logger logger = LoggerFactory.getLogger(MdJobRemotingUtil.class);

    public static final String MD_JOB_ACCESS_TOKEN = "MD-JOB-ACCESS-TOKEN";

    private static final TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[]{};
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }
    }};

    /**
     * 信任该http连接
     *
     * @param connection 连接
     */
    private static void trustAllHosts(HttpsURLConnection connection) {
        try {
            SSLContext tls = SSLContext.getInstance("TLS");
            tls.init(null, trustAllCerts, new java.security.SecureRandom());
            SSLSocketFactory socketFactory = tls.getSocketFactory();
            connection.setSSLSocketFactory(socketFactory);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        connection.setHostnameVerifier(new HostnameVerifier() {
            @Override
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        });
    }

    /**
     * 发送post消息
     */
    public static ReturnT postBody(String url, String accessToken, int timeout, Object reqeustObj, Class returnTargClassOfT) {
        HttpURLConnection connection = null;
        BufferedReader reader = null;
        try {
            URL realUrl = new URL(url);
            connection = (HttpURLConnection) realUrl.openConnection();
            boolean useHttps = realUrl.getProtocol().equals("https");
            if (useHttps) {
                HttpsURLConnection https = (HttpsURLConnection) connection;
                trustAllHosts(https);
            }
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setUseCaches(false);
            connection.setReadTimeout(timeout * 1000);
            connection.setConnectTimeout(3 * 1000);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("Accept-Charset", "application/json;charset=UTF-8");
            // 判断token
            if (accessToken != null && !accessToken.trim().isEmpty()) {
                connection.setRequestProperty(MD_JOB_ACCESS_TOKEN, accessToken);
            }
            // 进行连接
            connection.connect();
            if (reqeustObj != null) {
                String requestBody = GsonTool.toJson(reqeustObj);
                DataOutputStream dataOutputStream = new DataOutputStream(connection.getOutputStream());
                dataOutputStream.write(requestBody.getBytes(StandardCharsets.UTF_8));
                dataOutputStream.flush();
                dataOutputStream.close();
            }

            int responseCode = connection.getResponseCode();
            if (responseCode != 200) {
                //设置失败结果
                return new ReturnT<String>(ReturnT.FAIL_CODE, "job remoting fail, responseCode(" + responseCode + ") invalid. for url : " + url);
            }

            reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
            String resultJson = result.toString();

            try {
                //转换为ReturnT对象，返回给用户
                ReturnT returnT = GsonTool.fromJson(resultJson, ReturnT.class, returnTargClassOfT);
                return returnT;
            } catch (Exception e) {
                logger.error("xxl-job remoting (url=" + url + ") response content invalid(" + resultJson + ").", e);
                return new ReturnT<String>(ReturnT.FAIL_CODE, "xxl-job remoting (url=" + url + ") response content invalid(" + resultJson + ").");
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return new ReturnT<String>(ReturnT.FAIL_CODE, "job remoting error(" + e.getMessage() + "), for url : " + url);
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
                if (connection != null) {
                    connection.disconnect();
                }
            } catch (Exception e2) {
                logger.error(e2.getMessage(), e2);
            }
        }
    }
}
