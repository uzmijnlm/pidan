package com.github.pidan.batch.runtime;

import com.github.pidan.batch.runtime.util.SerializableUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class ExecutorBackend {
    private final Executor executor;
    private final SocketAddress driverManagerAddress;
    private Channel channel;

    public ExecutorBackend(Executor executor, SocketAddress driverManagerAddress) {
        this.executor = executor;
        this.driverManagerAddress = driverManagerAddress;
    }

    public void start(InetSocketAddress shuffleServiceBindAddress) {
        final ExecutorNetHandler handler = new ExecutorNetHandler();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch)
                            throws Exception {
                        ch.pipeline().addLast(handler);
                    }
                });

        try {
            bootstrap.connect(driverManagerAddress)
                    .addListener((ChannelFutureListener) future -> {
                        this.channel = future.channel();
                        writeEvent(channel, new ExecutorInitSuccessEvent(shuffleServiceBindAddress));
                    }).sync()
                    .channel().closeFuture()
                    .addListener((ChannelFutureListener) future -> {
                        workerGroup.shutdownGracefully();
                    });
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private class ExecutorNetHandler
            extends LengthFieldBasedFrameDecoder {
        public ExecutorNetHandler() {
            super(6553600, 0, 4);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in)
                throws Exception {
            in = (ByteBuf) super.decode(ctx, in);
            if (in == null) {
                return null;
            }

            int len = in.readInt();
            byte[] bytes = new byte[len];
            in.readBytes(bytes);
            ReferenceCountUtil.release(in);

            Task<?> task = SerializableUtil.byteToObject(bytes);
            executor.runTask(task);
            return task;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

        }
    }

    public synchronized void updateState(Event event)
            throws IOException
    {
        writeEvent(channel, event);
    }

    private static void writeEvent(Channel channel, Event event)
            throws IOException
    {
        ByteBuf buffer = channel.alloc().buffer();
        byte[] bytes = SerializableUtil.serialize(event);
        buffer.writeInt(bytes.length).writeBytes(bytes);
        channel.writeAndFlush(buffer);
    }
}
