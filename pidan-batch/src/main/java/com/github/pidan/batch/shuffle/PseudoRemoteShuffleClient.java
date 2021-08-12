package com.github.pidan.batch.shuffle;

import com.github.pidan.core.util.SerializableUtil;
import com.google.common.collect.Iterators;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class PseudoRemoteShuffleClient implements ShuffleClient {
    static final ByteBuf STOP_DOWNLOAD = Unpooled.EMPTY_BUFFER;
    private final Map<Integer, Map<Integer, InetSocketAddress>> dependMapTasks;
    private final List<NioEventLoopGroup> eventLoopGroups = new ArrayList<>();

    public PseudoRemoteShuffleClient(Map<Integer, Map<Integer, InetSocketAddress>> dependMapTasks) {
        this.dependMapTasks = dependMapTasks;
    }

    @Override
    public Iterator<?> read(int index, int stageId) {
        Map<Integer, InetSocketAddress> mapTaskIds = dependMapTasks.get(stageId);
        List<ShuffleClientHandler> handlers = new ArrayList<>();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        eventLoopGroups.add(workerGroup);
        for (Map.Entry<Integer, InetSocketAddress> entry : mapTaskIds.entrySet()) {
            ShuffleClientHandler shuffleClientHandler = new ShuffleClientHandler(stageId, index, entry.getKey());
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new HeaderEventHandler(shuffleClientHandler))
                                    .addLast(shuffleClientHandler);
                        }
                    });
            try {
                ChannelFuture future = bootstrap.connect(entry.getValue()).sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            handlers.add(shuffleClientHandler);
        }

        return Iterators.concat(handlers.stream().map(ShuffleClientHandler::getReader).collect(Collectors.toList()).iterator());
    }

    @Override
    public void close() {
        for (NioEventLoopGroup group : eventLoopGroups) {
            group.shutdownGracefully();
        }
    }

    private static class HeaderEventHandler extends ChannelInboundHandlerAdapter {
        private final ShuffleClientHandler shuffleClientHandler;
        private Integer awaitDownLoadSize;

        private HeaderEventHandler(ShuffleClientHandler shuffleClientHandler) {
            this.shuffleClientHandler = shuffleClientHandler;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception {
            ByteBuf in1 = (ByteBuf) msg;
            if (awaitDownLoadSize == null) {
                this.awaitDownLoadSize = in1.readInt();
                if (this.awaitDownLoadSize == 0) {
                    ReferenceCountUtil.release(in1);
                    ctx.fireChannelReadComplete();
                    shuffleClientHandler.finish();
                    return;
                }
            }
            int readableBytes = in1.readableBytes();
            if (readableBytes == 0) {
                ReferenceCountUtil.release(in1);
                return;
            }
            awaitDownLoadSize -= readableBytes;
            ctx.fireChannelRead(in1);
            if (awaitDownLoadSize <= 0) {
                ctx.fireChannelReadComplete();
                shuffleClientHandler.finish();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            shuffleClientHandler.exceptionCaught(ctx, cause);
        }
    }

    private static class ByteBufIteratorReader extends InputStream implements Iterator<Object> {
        private final BlockingQueue<ByteBuf> buffer = new LinkedBlockingQueue<>(10);
        private final DataInputStream dataInputStream = new DataInputStream(this);
        private ByteBuf byteBuf;
        private volatile Throwable cause;
        private boolean done = false;

        private void push(ByteBuf byteBuf)
                throws InterruptedException {
            buffer.put(byteBuf);
        }

        public void downloadFailed(Throwable e)
                throws InterruptedException {
            this.cause = e;
            buffer.clear();
            buffer.put(STOP_DOWNLOAD);
        }

        private ByteBufIteratorReader() {

        }

        @Override
        public int read() {
            if (byteBuf.readableBytes() == 0) {
                ReferenceCountUtil.release(byteBuf);
                try {
                    byteBuf = buffer.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(cause);
                }
            }
            return byteBuf.readByte() & 0xFF;
        }

        @Override
        public boolean hasNext() {
            if (done) {
                return false;
            }
            try {
                if (byteBuf == null) {
                    byteBuf = buffer.take();
                } else if (byteBuf.readableBytes() == 0) {
                    ReferenceCountUtil.release(byteBuf);
                    byteBuf = buffer.take();
                }
                if (byteBuf == STOP_DOWNLOAD) {
                    done = true;
                    if (cause != null) {
                        throw new RuntimeException(cause);
                    }
                    return false;
                }
            } catch (InterruptedException e) {
                done = true;
                throw new RuntimeException(e);
            }
            return true;
        }

        @Override
        public Object next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                int length = dataInputStream.readInt();
                byte[] bytes = new byte[length];
                dataInputStream.read(bytes);
                return SerializableUtil.byteToObject(bytes);
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

    }


    private static class ShuffleClientHandler
            extends ChannelInboundHandlerAdapter {
        private final int mapId;
        private final int stageId;
        private final int reduceId;
        private final ByteBufIteratorReader reader;

        private ChannelHandlerContext ctx;

        private ShuffleClientHandler(int stageId, int reduceId, int mapId) {
            this.mapId = mapId;
            this.stageId = stageId;
            this.reduceId = reduceId;
            this.reader = new ByteBufIteratorReader();
        }

        public ByteBufIteratorReader getReader() {
            return reader;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            this.ctx = ctx;
            //begin
            ByteBuf byteBuf = ctx.alloc().buffer(Integer.BYTES * 3, Integer.BYTES * 3);
            byteBuf.writeInt(stageId);
            byteBuf.writeInt(reduceId);
            byteBuf.writeInt(mapId);
            ctx.writeAndFlush(byteBuf);
        }

        public void finish() throws InterruptedException {
            reader.push(STOP_DOWNLOAD);
            ctx.close();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception {
            ByteBuf in1 = (ByteBuf) msg;
            if (in1.readableBytes() > 0) {
                reader.push(in1);
            } else {
                ReferenceCountUtil.release(in1);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            ctx.close();
            reader.downloadFailed(cause);
        }
    }
}
