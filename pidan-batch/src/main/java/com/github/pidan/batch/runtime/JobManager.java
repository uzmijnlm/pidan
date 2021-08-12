package com.github.pidan.batch.runtime;

import com.github.pidan.batch.runtime.event.Event;
import com.github.pidan.batch.runtime.event.ExecutorInitSuccessEvent;
import com.github.pidan.batch.runtime.event.TaskEvent;
import com.github.pidan.core.util.SerializableUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static com.github.pidan.core.configuration.Constant.JOB_MANAGER_PORT;

public class JobManager {
    private final ChannelFuture future;
    private final int executorNum;
    private final InetSocketAddress bindAddress;
    private final ConcurrentMap<InetSocketAddress, DriverNetManagerHandler> executorHandlers = new ConcurrentHashMap<>();
    private final BlockingQueue<TaskEvent> queue = new LinkedBlockingQueue<>(65536);

    public JobManager(int executorNum) {
        this.executorNum = executorNum;

        NioEventLoopGroup boosGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(boosGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new DriverNetManagerHandler());
                    }
                });
        try {
            this.future = serverBootstrap.bind(JOB_MANAGER_PORT).sync();
            int bindPort = ((InetSocketAddress) future.channel().localAddress()).getPort();
            this.bindAddress = InetSocketAddress.createUnresolved(InetAddress.getLocalHost().getHostName(), bindPort);
            future.channel().closeFuture().addListener((ChannelFutureListener) channelFuture -> {
                boosGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            });
        } catch (InterruptedException | UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public InetSocketAddress getBindAddress() {
        return bindAddress;
    }

    public InetSocketAddress submitTask(Task<?> task) {
        List<InetSocketAddress> addresses = new ArrayList<>(executorHandlers.keySet());
        //cache算子会使得Executor节点拥有状态，调度时应注意幂等
        InetSocketAddress address = addresses.get(task.getTaskId() % executorNum);
        executorHandlers.get(address).submitTask(task);
        return address;
    }

    public void awaitAllExecutorRegistered() {
        while (executorHandlers.size() != executorNum) {
            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public TaskEvent awaitTaskEvent() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        future.channel().close();
    }

    private class DriverNetManagerHandler extends LengthFieldBasedFrameDecoder {
        private ChannelHandlerContext executorChannel;
        private SocketAddress socketAddress;

        public DriverNetManagerHandler() {
            super(1048576, 0, 4);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            this.executorChannel = ctx;
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            in = (ByteBuf) super.decode(ctx, in);
            if (in == null) {
                return null;
            }
            int len = in.readInt();
            byte[] bytes = new byte[len];
            in.readBytes(bytes);
            ReferenceCountUtil.release(in);
            Event event;
            try {
                event = SerializableUtil.byteToObject(bytes);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (event instanceof ExecutorInitSuccessEvent) {
                InetSocketAddress shuffleService = ((ExecutorInitSuccessEvent) event).getShuffleServiceAddress();
                executorHandlers.put(shuffleService, this);
            } else if (event instanceof TaskEvent) {
                queue.put((TaskEvent) event);
            } else {
                throw new UnsupportedOperationException();
            }
            return event;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        }

        public void submitTask(Task<?> task) {
            ByteBuf buffer = executorChannel.alloc().buffer();
            byte[] bytes;
            try {
                bytes = SerializableUtil.serialize(task);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            buffer.writeInt(bytes.length).writeBytes(bytes);
            executorChannel.writeAndFlush(buffer);
        }
    }
}
