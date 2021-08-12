package com.github.pidan.batch.runtime;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;

import static com.github.pidan.core.configuration.Constant.*;

public class ShuffleManagerService {

    private final InetSocketAddress shuffleServiceBindAddress;
    private final ChannelFuture future;

    public ShuffleManagerService() throws UnknownHostException, InterruptedException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                FileUtils.deleteDirectory(new File(SHUFFLE_DATA_DIRECTORY));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
        final NioEventLoopGroup boosGroup = new NioEventLoopGroup(1);
        final NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(boosGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new ShuffleServiceHandler());
                    }
                });
        this.future = serverBootstrap.bind(SHUFFLE_SERVICE_PORT).sync();
        future.channel().closeFuture().addListener((ChannelFutureListener) channelFuture -> {
            boosGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        });

        int bindPort = ((InetSocketAddress) future.channel().localAddress()).getPort();
        this.shuffleServiceBindAddress = InetSocketAddress.createUnresolved(InetAddress.getLocalHost().getHostName(), bindPort);
    }

    public InetSocketAddress getShuffleServiceBindAddress() {
        return shuffleServiceBindAddress;
    }


    public void join() throws InterruptedException {
        future.channel().closeFuture().sync();
    }

    private static class ShuffleServiceHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf in = (ByteBuf) msg;
            int shuffleId = in.readInt();
            int reduceId = in.readInt();
            int mapId = in.readInt();
            ReferenceCountUtil.release(msg);

            File shuffleFile = new File(SHUFFLE_DATA_DIRECTORY + SHUFFLE_FILE_PREFIX + shuffleId + "_" + mapId + "_" + reduceId + ".data");
            FileInputStream fileInputStream = new FileInputStream(shuffleFile);
            FileChannel fileChannel = fileInputStream.getChannel();
            long position = fileChannel.position();
            long count = fileChannel.size();
            ctx.writeAndFlush(new DefaultFileRegion(fileChannel, position, count))
                    .addListener((ChannelFutureListener) future -> fileInputStream.close());
        }
    }

    public void stop() {
        future.channel().close();
    }
}
