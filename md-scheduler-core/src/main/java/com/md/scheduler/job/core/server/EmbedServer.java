package com.md.scheduler.job.core.server;

import com.md.scheduler.job.core.biz.ExecutorBiz;
import com.md.scheduler.job.core.biz.impl.ExecutorBizImpl;
import com.md.scheduler.job.core.biz.model.ReturnT;
import com.md.scheduler.job.core.biz.model.TriggerParam;
import com.md.scheduler.job.core.thread.ExecutorRegistryThread;
import com.md.scheduler.job.core.util.GsonTool;
import com.md.scheduler.job.core.util.MdJobRemotingUtil;
import com.md.scheduler.job.core.util.ThrowableUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.sctp.nio.NioSctpServerChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

/**
 * 执行器内嵌的netty服务器
 */
public class EmbedServer {

    private static final Logger logger = LoggerFactory.getLogger(EmbedServer.class);

    /**
     * 执行器接口，在start方法中初始化
     */
    private ExecutorBiz executorBiz;
    /**
     * 启动netty服务器的线程
     */
    private Thread thread;

    public void start(final String address, final int port, final String appName, final String accessToken) {
        executorBiz = new ExecutorBizImpl();
        thread = new Thread(() -> {
            EventLoopGroup bossGroup = new NioEventLoopGroup();
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            ThreadPoolExecutor bizThreadPool = new ThreadPoolExecutor(
                    0,
                    200,
                    60L,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(1024),
                    r -> new Thread(r, "job, EmbedServer bizThreadPool-" + r.hashCode()),
                    (r, executor) -> {
                        throw new RuntimeException("job, EmbedServer bizThreadPool is EXHAUSTED!");
                    }
            );
            try {
                ServerBootstrap serverBootstrap = new ServerBootstrap();
                serverBootstrap.group(bossGroup, workerGroup)
                        .channel(NioSctpServerChannel.class)
                        .childHandler(new ChannelInitializer<SocketChannel>() {

                            @Override
                            protected void initChannel(SocketChannel channel) throws Exception {
                                channel.pipeline()
                                        //心跳检测
                                        .addLast(new IdleStateHandler(0, 0, 30 * 3, TimeUnit.SECONDS))
                                        .addLast(new HttpServerCodec())
                                        .addLast(new HttpObjectAggregator(5 * 1024 * 1024))
                                        .addLast(new EmbedHttpServerHandler(executorBiz, accessToken, bizThreadPool));

                            }
                        }).childOption(ChannelOption.SO_KEEPALIVE, true);
                //绑定端口号
                ChannelFuture future = serverBootstrap.bind(port).sync();
                logger.info(">>>>>>>>>>> job remoting server start success, nettype = {}, port = {}", EmbedServer.class, port);
                //注册执行器到调度中心
                startRegistry(appName, address);
                //等待关闭
                future.channel().closeFuture().sync();
            } catch (InterruptedException e) {
                logger.info(">>>>>>>>>>> job remoting server stop.");
            } catch (Exception e) {
                logger.error(">>>>>>>>>>> job remoting server error.", e);
            } finally {
                try {
                    workerGroup.shutdownGracefully();
                    bossGroup.shutdownGracefully();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * 销毁资源
     *
     * @throws Exception
     */
    public void stop() throws Exception {
        if (thread != null && thread.isAlive()) {
            thread.interrupt();
        }
        stopRegistry();
        logger.info(">>>>>>>>>>> job remoting server destroy success.");
    }

    /**
     * 内嵌服务器，接收服务端下发的任务调用
     */
    public static class EmbedHttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        private static Logger logger = LoggerFactory.getLogger(EmbedHttpServerHandler.class);

        private ExecutorBiz executorBiz;

        private String accessToken;

        private ThreadPoolExecutor bizThreadPool;

        public EmbedHttpServerHandler(ExecutorBiz executorBiz, String accessToken, ThreadPoolExecutor bizThreadPool) {
            this.executorBiz = executorBiz;
            this.accessToken = accessToken;
            this.bizThreadPool = bizThreadPool;
        }

        /**
         * 执行任务的调用
         *
         * @param channelHandlerContext
         * @param fullHttpRequest
         * @throws Exception
         */
        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, FullHttpRequest fullHttpRequest) throws Exception {
            String requestData = fullHttpRequest.content().toString(StandardCharsets.UTF_8);
            String uri = fullHttpRequest.uri();
            HttpMethod httpMethod = fullHttpRequest.method();
            boolean keepAlive = HttpUtil.isKeepAlive(fullHttpRequest);
            String accessTokenReq = fullHttpRequest.headers().get(MdJobRemotingUtil.MD_JOB_ACCESS_TOKEN);

            bizThreadPool.execute(() -> {
                Object responseObj = process(httpMethod, uri, requestData, accessTokenReq);
                String responseJson = GsonTool.toJson(responseObj);
                writeResponse(channelHandlerContext, keepAlive, responseJson);
            });
        }

        private Object process(HttpMethod httpMethod, String url, String requestData, String accessToken) {
            if (HttpMethod.POST != httpMethod) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, HttpMethod not support.");
            }
            if (url == null || url.trim().length() == 0) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping empty.");
            }
            if (accessToken != null && accessToken.trim().length() > 0 && !this.accessToken.equals(accessToken)) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "The access token is wrong.");
            }
            try {
                switch (url) {
                    case "/run":
                        TriggerParam triggerParam = GsonTool.fromJson(requestData, TriggerParam.class);
                        return executorBiz.run(triggerParam);
                    default:
                        return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping(" + url
                                + ") not found.");
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return new ReturnT<String>(ReturnT.FAIL_CODE, "request error:" + ThrowableUtil.toString(e));
            }
        }

        private void writeResponse(ChannelHandlerContext channelHandlerContext, boolean keepAlive, String responseJson) {
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(responseJson.getBytes(StandardCharsets.UTF_8)));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html;charset=UTF-8");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            if (keepAlive) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
            channelHandlerContext.writeAndFlush(response);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error(">>>>>>>>>>> job provider netty_http server caught exception", cause);
            ctx.close();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                ctx.channel().close();
                logger.debug(">>>>>>>>>>> job provider netty_http server idle event");
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }

    }

    /**
     * 启动线程，注册执行器到调度中心
     *
     * @param appName
     * @param address
     */
    public void startRegistry(final String appName, final String address) {
        // 启动线程，注册执行器到调度中心
        ExecutorRegistryThread.getInstance().start(appName, address);
    }

    public void stopRegistry() {
        ExecutorRegistryThread.getInstance().toStop();
    }

}
