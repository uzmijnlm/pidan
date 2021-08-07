package com.github.pidan.batch.runtime;

import com.github.pidan.batch.runtime.util.SerializableUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.*;

public class JobManager {
    private final ChannelFuture future;
    private final int executorNum;
    private final InetSocketAddress bindAddress;
    private final ConcurrentMap<SocketAddress, DriverNetManagerHandler> executorHandlers = new ConcurrentHashMap<>();
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
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new DriverNetManagerHandler());
                    }
                });
        try {
            this.future = serverBootstrap.bind(0).sync();
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

    public void awaitAllExecutorRegistered() {
        while (executorHandlers.size() != executorNum) {
            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private class DriverNetManagerHandler
            extends LengthFieldBasedFrameDecoder {
        private ChannelHandlerContext executorChannel;
        private SocketAddress socketAddress;

        public DriverNetManagerHandler() {
            super(1048576, 0, 4);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
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
            Event event = SerializableUtil.byteToObject(bytes);
            if (event instanceof ExecutorInitSuccessEvent) {
                SocketAddress shuffleService = ((ExecutorInitSuccessEvent) event).getShuffleServiceAddress();
                executorHandlers.put(shuffleService, this);
            } else if (event instanceof TaskEvent) {
                queue.put((TaskEvent) event);
            } else {
                throw new UnsupportedOperationException();
            }
            return event;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        }

        public void submitTask(Task<?> task) {
            ByteBuf buffer = executorChannel.alloc().buffer();
            byte[] bytes;
            bytes = SerializableUtil.serialize(task);
            buffer.writeInt(bytes.length).writeBytes(bytes);
            executorChannel.writeAndFlush(buffer);
        }
    }
}
