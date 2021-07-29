package com.tunan.netty.echo;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

/**
 * @author Tunan
 */
public class EchoServer {

    private final int port;

    public EchoServer(int port) {
        this.port = port;
    }

    public static void main(String[] args) {

        if(args.length != 1){
            System.err.println(
                    "Usage: "+EchoServer.class.getSimpleName()+" <port>");
            return;
        }

        int port = Integer.parseInt(args[0]);
        try {
            new EchoServer(port).start();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void start() throws InterruptedException {
        final EchoServerHandler handler = new EchoServerHandler();
        NioEventLoopGroup group = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(group)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(new InetSocketAddress(port))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel sc) throws Exception {
                                 sc.pipeline().addLast(handler);
                        }
                    });
            ChannelFuture f = b.bind().sync();
            f.channel().closeFuture().sync();

        } finally {
            group.shutdownGracefully().sync();
        }
    }
}
